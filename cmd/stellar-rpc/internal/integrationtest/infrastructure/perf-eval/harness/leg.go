package harness

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Leg is the env-derived context shared by every on-box leg runner: the box
// identity/result wiring from the user-data preamble plus the S3 handle.
type Leg struct {
	Title       string
	Bucket      string
	WorkDir     string
	ResultsFile string
	ResultKey   string
	TargetSHA   string
	RunID       string
	RepoRoot    string // cwd: run_leg starts runners at the repo checkout root
	Fetch       *S3Fetcher
}

// LegSetup reads the common leg env and builds the S3 client. On failure it
// has already written the bail body, so the runner just returns the error.
func LegSetup(ctx context.Context, title string) (*Leg, error) {
	l := &Leg{
		Title:       title,
		Bucket:      Env("BUCKET", "stellar-rpc-ci-load-test"),
		WorkDir:     Env("WORK_DIR", "/data"),
		ResultsFile: Env("RESULTS_FILE", "/tmp/results.md"),
		ResultKey:   os.Getenv("RESULT_KEY"),
		TargetSHA:   os.Getenv("TARGET_SHA"),
		RunID:       Env("RUN_ID", "manual"),
	}
	var err error
	if l.RepoRoot, err = os.Getwd(); err != nil {
		return nil, l.Bail("getwd: %v", err)
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(Env("REGION", "us-east-1")))
	if err != nil {
		return nil, l.Bail("loading AWS config: %v", err)
	}
	l.Fetch = &S3Fetcher{Client: s3.NewFromConfig(awsCfg), Bucket: l.Bucket}
	return l, nil
}

// Bail writes the leg's failure body and returns the error for the runner to
// exit with (run_leg then publishes the fail verdict).
func (l *Leg) Bail(format string, args ...any) error {
	return BailInstance(l.ResultsFile, l.Title, l.RunID, l.TargetSHA, fmt.Sprintf(format, args...))
}

// Publish uploads the ok verdict with the leg's results markdown (and bench
// JSON when benchPath is non-empty).
func (l *Leg) Publish(ctx context.Context, benchPath string) error {
	return PublishResult(ctx, l.Fetch.Client, l.Bucket, l.ResultKey, "ok",
		l.RunID, l.TargetSHA, l.ResultsFile, benchPath)
}

// corePath is where every leg installs the pre-built stellar-core cached in S3.
const corePath = "/usr/local/bin/stellar-core"

// FetchStellarCore streams the cached stellar-core build from S3 into place
// and returns its path.
func (f *S3Fetcher) FetchStellarCore(ctx context.Context) (string, error) {
	if err := f.FetchVerified(ctx, "core/stellar-core.zst", corePath, true, "stellar-core"); err != nil {
		return "", err
	}
	if err := os.Chmod(corePath, 0o755); err != nil {
		return "", fmt.Errorf("chmod stellar-core: %w", err)
	}
	return corePath, nil
}
