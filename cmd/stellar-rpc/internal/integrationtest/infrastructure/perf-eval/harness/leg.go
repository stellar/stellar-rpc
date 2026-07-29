package harness

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/caarlos0/env/v11"
)

// Leg is the env-derived context shared by every on-box leg runner.
type Leg struct {
	Title       string
	Bucket      string `env:"BUCKET"       envDefault:"stellar-rpc-ci-load-test"`
	Region      string `env:"REGION"       envDefault:"us-east-1"`
	WorkDir     string `env:"WORK_DIR"     envDefault:"/data"`
	ResultsFile string `env:"RESULTS_FILE" envDefault:"/tmp/results.md"`
	ResultKey   string `env:"RESULT_KEY"`
	TargetSHA   string `env:"TARGET_SHA"`
	RunID       string `env:"RUN_ID"       envDefault:"manual"`
	RepoRoot    string // cwd: run_leg starts runners at the repo checkout root
	Fetch       *S3Fetcher
}

// LegSetup reads the common leg env and builds the S3 client. On failure it
// has already written the bail body, so the runner just returns the error.
func LegSetup(ctx context.Context, title string) (*Leg, error) {
	l := &Leg{Title: title}
	if err := env.Parse(l); err != nil {
		return nil, fmt.Errorf("parsing leg env: %w", err)
	}
	var err error
	if l.RepoRoot, err = os.Getwd(); err != nil {
		return nil, l.Bail("getwd: %v", err)
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(l.Region))
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
