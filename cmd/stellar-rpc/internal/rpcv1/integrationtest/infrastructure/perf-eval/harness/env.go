package harness

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/caarlos0/env/v11"
)

// PollerConfig is the environment shared by the GHA-side commands Gather and
// Relay. Gather and Relay embed it; the type is exported because the env
// parser fills only exported embedded fields.
type PollerConfig struct {
	InstanceID         string  `env:"INSTANCE_ID,required,notEmpty"`
	Region             string  `env:"AWS_REGION,required,notEmpty"`
	GitHubOutput       string  `env:"GITHUB_OUTPUT,required,notEmpty"`
	Bucket             string  `env:"BUCKET,required,notEmpty"`
	ResultKey          string  `env:"RESULT_KEY,required,notEmpty"`
	RunID              string  `env:"RUN_ID,required,notEmpty"`
	PollInterval       seconds `env:"POLL_INTERVAL,required,notEmpty"`
	DebugLogLines      count   `env:"DEBUG_LOG_LINES,required,notEmpty"`
	DebugLogEveryPolls count   `env:"DEBUG_LOG_EVERY_POLLS,required,notEmpty"`
}

// seconds is a duration the environment gives as a whole, positive number of
// seconds.
type seconds time.Duration

func (s *seconds) UnmarshalText(text []byte) error {
	n, err := parsePositive(text)
	if err != nil {
		return err
	}
	*s = seconds(time.Duration(n) * time.Second)
	return nil
}

func (s *seconds) duration() time.Duration { return time.Duration(*s) }

// count is a positive integer.
type count int

func (c *count) UnmarshalText(text []byte) error {
	n, err := parsePositive(text)
	if err != nil {
		return err
	}
	*c = count(n)
	return nil
}

// unixTime is an instant the environment gives as positive Unix seconds.
type unixTime time.Time

func (u *unixTime) UnmarshalText(text []byte) error {
	n, err := parsePositive(text)
	if err != nil {
		return err
	}
	*u = unixTime(time.Unix(n, 0))
	return nil
}

// parsePositive reads a base-10 integer of at least one.
func parsePositive(text []byte) (int64, error) {
	n, err := strconv.ParseInt(string(text), 10, 64)
	if err != nil {
		return 0, err
	}
	if n < 1 {
		return 0, fmt.Errorf("must be positive, got %d", n)
	}
	return n, nil
}

// loadEnv fills cfg, a pointer to a config struct, from the environment. Every
// error names the environment variable at fault.
func loadEnv(cfg any) error {
	err := env.Parse(cfg)
	if err == nil {
		return nil
	}
	var aggregate env.AggregateError
	cfgType := reflect.TypeOf(cfg)
	if !errors.As(err, &aggregate) || cfgType.Kind() != reflect.Pointer {
		return err
	}
	if cfgType.Elem().Kind() != reflect.Struct {
		return err
	}
	errs := make([]error, 0, len(aggregate.Errors))
	for _, one := range aggregate.Errors {
		errs = append(errs, namedEnvError(cfgType.Elem(), one))
	}
	return errors.Join(errs...)
}

// namedEnvError replaces the Go field name a parse failure carries with the
// environment variable that field reads. Other failures pass through.
func namedEnvError(structType reflect.Type, err error) error {
	var parseErr env.ParseError
	if !errors.As(err, &parseErr) {
		return err
	}
	field, ok := structType.FieldByName(parseErr.Name)
	if !ok {
		return err
	}
	key, _, _ := strings.Cut(field.Tag.Get("env"), ",")
	return fmt.Errorf("%s: %w", key, parseErr.Err)
}

// newResultPoller loads the AWS config for cfg.Region and builds the S3 client
// and SSM runner the poller and its diagnostics use.
func newResultPoller(ctx context.Context, cfg PollerConfig) (*resultPoller, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}
	return &resultPoller{
		s3Client:        s3.NewFromConfig(awsCfg),
		runner:          &ssmRunner{client: ssm.NewFromConfig(awsCfg), instanceID: cfg.InstanceID},
		bucket:          cfg.Bucket,
		key:             cfg.ResultKey,
		runID:           cfg.RunID,
		interval:        cfg.PollInterval.duration(),
		debugLogLines:   int(cfg.DebugLogLines),
		debugEveryPolls: int(cfg.DebugLogEveryPolls),
	}, nil
}
