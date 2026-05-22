// Uploads cold-store .pack files from a local directory to a GCS bucket
// under a configurable prefix. Uses Application Default Credentials
// (~/.config/gcloud/application_default_credentials.json) — the same
// auth path the backfill script uses for downloads, so no `gcloud auth login`
// required.
//
// Resume: an object that already exists at the destination with the same
// size as the local file is skipped. Mismatched-size objects are
// re-uploaded (overwrite). CRC32C is computed locally and sent in the
// upload metadata so GCS verifies the round-trip; a CRC mismatch fails
// the upload.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

func main() {
	var (
		srcDir     string
		bucketName string
		prefix     string
		workers    int
		dryRun     bool
	)
	flag.StringVar(&srcDir, "src-dir", "/mnt/nvme/disk2/ledgers/cold",
		"local cold-store root (.pack files under {bucketID}/{chunkID}.pack)")
	flag.StringVar(&bucketName, "bucket", "rpc-full-history",
		"destination GCS bucket (without gs:// prefix)")
	flag.StringVar(&prefix, "prefix", "cold",
		"object name prefix under the bucket; final object name is {prefix}/{bucketID}/{chunkID}.pack")
	flag.IntVar(&workers, "workers", 8,
		"concurrent upload workers")
	flag.BoolVar(&dryRun, "dry-run", false,
		"list what would be uploaded without uploading")
	flag.Parse()

	if srcDir == "" || bucketName == "" {
		fmt.Fprintln(os.Stderr, "--src-dir and --bucket are required")
		os.Exit(2)
	}
	if workers < 1 {
		fmt.Fprintln(os.Stderr, "--workers must be >= 1")
		os.Exit(2)
	}
	prefix = strings.Trim(prefix, "/")

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.WithError(err).Error("storage.NewClient failed")
		os.Exit(1)
	}
	defer client.Close()
	bucket := client.Bucket(bucketName)

	files, err := listPackFiles(srcDir)
	if err != nil {
		logger.WithError(err).Errorf("listing %s", srcDir)
		os.Exit(1)
	}
	logger.Infof("found %d .pack files under %s", len(files), srcDir)

	existing, err := listRemoteSizes(ctx, bucket, prefix)
	if err != nil {
		logger.WithError(err).Errorf("listing gs://%s/%s/", bucketName, prefix)
		os.Exit(1)
	}
	logger.Infof("found %d existing objects under gs://%s/%s/", len(existing), bucketName, prefix)

	if dryRun {
		todo := 0
		for _, f := range files {
			objName := objectName(prefix, f.rel)
			if existing[objName] != f.size {
				todo++
			}
		}
		logger.Infof("dry-run: would upload %d / %d files", todo, len(files))
		return
	}

	if err := runUpload(ctx, logger, bucket, bucketName, prefix, files, existing, workers); err != nil {
		logger.WithError(err).Error("upload failed")
		os.Exit(1)
	}
}

type packFile struct {
	abs  string // absolute filesystem path
	rel  string // path relative to srcDir, slash-separated
	size int64
}

// listPackFiles walks srcDir for *.pack files, returning their paths
// (sorted by rel) and sizes.
func listPackFiles(srcDir string) ([]packFile, error) {
	var files []packFile
	err := filepath.WalkDir(srcDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".pack") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		files = append(files, packFile{
			abs:  path,
			rel:  filepath.ToSlash(rel),
			size: info.Size(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return files, nil
}

// listRemoteSizes returns objectName -> size for every object under
// {prefix}/ in the bucket. We do a single list rather than per-file
// Attrs calls so resume on 1000+ files takes seconds, not minutes.
func listRemoteSizes(ctx context.Context, bucket *storage.BucketHandle, prefix string) (map[string]int64, error) {
	out := make(map[string]int64)
	queryPrefix := prefix
	if queryPrefix != "" {
		queryPrefix += "/"
	}
	it := bucket.Objects(ctx, &storage.Query{Prefix: queryPrefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		out[attrs.Name] = attrs.Size
	}
	return out, nil
}

func objectName(prefix, rel string) string {
	if prefix == "" {
		return rel
	}
	return prefix + "/" + rel
}

func runUpload(
	ctx context.Context,
	logger *supportlog.Entry,
	bucket *storage.BucketHandle,
	bucketName, prefix string,
	files []packFile,
	existing map[string]int64,
	workers int,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	total := len(files)
	fileCh := make(chan packFile, workers*2)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
		done     atomic.Int64
		skipped  atomic.Int64
		uploaded atomic.Int64
		bytesUp  atomic.Int64
	)

	runStart := time.Now()
	for w := range workers {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			wLog := logger.WithField("worker", workerID)
			for f := range fileCh {
				if ctx.Err() != nil {
					return
				}
				objName := objectName(prefix, f.rel)

				if sz, ok := existing[objName]; ok && sz == f.size {
					skipped.Add(1)
					done.Add(1)
					maybeLogProgress(wLog, done.Load(), int64(total), skipped.Load(), uploaded.Load(), bytesUp.Load(), runStart)
					continue
				}

				n, err := uploadOne(ctx, bucket, objName, f)
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("upload %s -> gs://%s/%s: %w", f.abs, bucketName, objName, err)
						cancel()
					})
					return
				}
				bytesUp.Add(n)
				uploaded.Add(1)
				done.Add(1)
				maybeLogProgress(wLog, done.Load(), int64(total), skipped.Load(), uploaded.Load(), bytesUp.Load(), runStart)
			}
		}()
	}

	for _, f := range files {
		select {
		case <-ctx.Done():
		case fileCh <- f:
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(fileCh)
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	logger.Infof(
		"upload complete: %d files (%d uploaded, %d skipped) in %s, %.1f GiB sent",
		total, uploaded.Load(), skipped.Load(),
		time.Since(runStart).Round(time.Second),
		float64(bytesUp.Load())/(1<<30),
	)
	return nil
}

// uploadOne streams f.abs to gs://bucket/objName. CRC32C is computed
// inline and set in the object's metadata so GCS verifies the
// server-side checksum matches.
func uploadOne(
	ctx context.Context,
	bucket *storage.BucketHandle,
	objName string,
	f packFile,
) (int64, error) {
	src, err := os.Open(f.abs)
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer src.Close()

	w := bucket.Object(objName).NewWriter(ctx)
	w.ChunkSize = 32 << 20 // 32 MiB
	w.ContentType = "application/octet-stream"

	// Compute CRC32C while streaming so GCS validates it on Write.
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	n, err := io.Copy(io.MultiWriter(w, h), src)
	if err != nil {
		_ = w.Close()
		return 0, fmt.Errorf("copy: %w", err)
	}
	w.SendCRC32C = true
	w.CRC32C = h.Sum32()
	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("close: %w", err)
	}
	if n != f.size {
		return 0, fmt.Errorf("copied %d bytes but expected %d", n, f.size)
	}
	return n, nil
}

func maybeLogProgress(
	logger *supportlog.Entry,
	done, total, skipped, uploaded, bytesUp int64,
	start time.Time,
) {
	if done%10 != 0 && done != total {
		return
	}
	elapsed := time.Since(start)
	gibUp := float64(bytesUp) / (1 << 30)
	gibPerSec := gibUp / elapsed.Seconds()
	logger.Infof(
		"progress: %d/%d (uploaded=%d skipped=%d) %.1f GiB sent in %s (%.1f GiB/min)",
		done, total, uploaded, skipped, gibUp,
		elapsed.Round(time.Second),
		gibPerSec*60,
	)
}
