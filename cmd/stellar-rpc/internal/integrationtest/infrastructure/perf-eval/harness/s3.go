package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/klauspost/compress/zstd"
)

// Result is the structured run outcome the box publishes to S3 as an atomic
// object. The gatherer polls for it and reads either a complete object or
// a 404.
type Result struct {
	SchemaVersion int             `json:"schemaVersion"`
	Verdict       string          `json:"verdict"` // "ok" or "fail"
	Markdown      string          `json:"markdown"`
	Bench         json.RawMessage `json:"bench,omitempty"`
	RunID         string          `json:"runId"`
	TargetSHA     string          `json:"targetSha"`
}

// PublishResult uploads the run result to s3://bucket/key as one atomic object
// that the gatherer can poll for.
func PublishResult(
	ctx context.Context,
	client *s3.Client,
	bucket, key, verdict, runID, targetSHA, mdPath, benchPath string,
) error {
	if key == "" {
		logger.Infof("RESULT_KEY unset; skipping S3 result publish (verdict: %s)", verdict)
		return nil
	}
	md, err := os.ReadFile(mdPath)
	if err != nil {
		return fmt.Errorf("reading result markdown %s: %w", mdPath, err)
	}
	res := Result{
		SchemaVersion: 1,
		Verdict:       verdict,
		Markdown:      string(md),
		RunID:         runID,
		TargetSHA:     targetSHA,
	}
	if benchPath != "" {
		if bench, berr := os.ReadFile(benchPath); berr == nil {
			res.Bench = json.RawMessage(bench)
		} else {
			logger.Warnf("bench results %s unavailable: %v", benchPath, berr) // best-effort for local runs
		}
	}
	body, err := json.Marshal(res)
	if err != nil {
		return fmt.Errorf("marshaling result: %w", err)
	}
	logger.Infof("publishing result to s3://%s/%s (verdict: %s, %d bytes)", bucket, key, verdict, len(body))
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	}); err != nil {
		return fmt.Errorf("uploading result: %w", err)
	}
	return nil
}

// ErrResultNotReady means the result object hasn't been published yet.
var ErrResultNotReady = errors.New("result not published yet")

// FetchResult gets and decodes the result object, returning ErrResultNotReady
// when it is absent.
func FetchResult(ctx context.Context, client *s3.Client, bucket, key string) (*Result, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrResultNotReady
		}
		return nil, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	var res Result
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("decoding result object: %w", err)
	}
	return &res, nil
}

// isNotFound reports whether a GetObject error means the key is absent.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	var apiErr smithy.APIError
	if errors.As(err, &nsk) ||
		errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
		return true
	}
	return false
}

// S3Fetcher streams objects from one bucket, sha-verifying when the object
// carries sha256-raw metadata.
type S3Fetcher struct {
	Client *s3.Client
	Bucket string
}

// FetchVerified downloads key to dst (zstd-decoding when zstdMode), checking its
// sha256 against the object's sha256-raw metadata when present.
func (f *S3Fetcher) FetchVerified(ctx context.Context, key, dst string, zstdMode bool, label string) error {
	expected := f.expectedSHA(ctx, key, label)
	logger.Infof("fetching %s", label)
	got, err := f.streamObject(ctx, key, dst, zstdMode)
	if err != nil {
		return fmt.Errorf("failed to download %s: %w", label, err)
	}
	if expected != "" && expected != got {
		return fmt.Errorf("%s hash mismatch: expected %s, got %s", label, expected, got)
	}
	if expected == "" {
		logger.Infof("%s hash computed (unverified) (%s)", label, got)
	} else {
		logger.Infof("%s hash OK (%s)", label, got)
	}
	return nil
}

// expectedSHA returns the object's sha256-raw metadata, or "" if the object or
// the key is absent (caller then fetches unverified).
func (f *S3Fetcher) expectedSHA(ctx context.Context, key, label string) string {
	head, err := f.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &f.Bucket, Key: &key})
	if err != nil {
		logger.Warnf("head-object failed for s3://%s/%s; fetching %s without checksum", f.Bucket, key, label)
		return ""
	}
	// S3 lowercases user-metadata keys; the SDK strips the x-amz-meta- prefix.
	if sha := head.Metadata["sha256-raw"]; sha != "" {
		return sha
	}
	logger.Warnf("no sha256-raw on s3://%s/%s; skipping %s checksum", f.Bucket, key, label)
	return ""
}

// streamObject downloads key to dst (zstd-decoding when zstdMode) and returns
// the sha256 of the bytes written.
func (f *S3Fetcher) streamObject(ctx context.Context, key, dst string, zstdMode bool) (string, error) {
	out, err := f.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &f.Bucket, Key: &key})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()

	var src io.Reader = out.Body
	if zstdMode {
		zr, err := zstd.NewReader(out.Body)
		if err != nil {
			return "", err
		}
		defer zr.Close()
		src = zr
	}

	file, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(file, hasher), src); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
