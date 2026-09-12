package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/caarlos0/env/v11"
)

// envTag is the struct tag the config structs use to name their variables.
const envTag = "env"

// pollerConfig is the environment shared by the GHA-side commands (Gather,
// Relay). Ints are bare seconds / counts, as the workflows set them.
type pollerConfig struct {
	InstanceID         string `env:"INSTANCE_ID,required,notEmpty"`
	Region             string `env:"AWS_REGION,required,notEmpty"`
	GitHubOutput       string `env:"GITHUB_OUTPUT,required,notEmpty"`
	Bucket             string `env:"BUCKET,required,notEmpty"`
	ResultKey          string `env:"RESULT_KEY,required,notEmpty"`
	RunID              string `env:"RUN_ID,required,notEmpty"`
	PollIntervalSecs   int    `env:"POLL_INTERVAL,required,notEmpty"`
	DebugLogLines      int    `env:"DEBUG_LOG_LINES,required,notEmpty"`
	DebugLogEveryPolls int    `env:"DEBUG_LOG_EVERY_POLLS,required,notEmpty"`
}

func (c *pollerConfig) validate() error {
	return requirePositiveInts(
		envInt{"POLL_INTERVAL", c.PollIntervalSecs},
		envInt{"DEBUG_LOG_LINES", c.DebugLogLines},
		envInt{"DEBUG_LOG_EVERY_POLLS", c.DebugLogEveryPolls},
	)
}

// pollInterval is the wait between two result fetches.
func (c *pollerConfig) pollInterval() time.Duration {
	return time.Duration(c.PollIntervalSecs) * time.Second
}

// envConfig is a pointer to a command's config struct. It checks the values
// the environment parser has already filled in.
type envConfig interface {
	validate() error
}

// loadEnv fills cfg from the environment and checks the parsed values. Every
// error names the environment variable at fault.
func loadEnv(cfg envConfig) error {
	if err := env.Parse(cfg); err != nil {
		return namedEnvError(cfg, err)
	}
	return cfg.validate()
}

// namedEnvError rewrites parse failures, which name the Go field, so that they
// name the environment variable the field reads. It joins the errors the
// parser reported for the whole struct.
func namedEnvError(cfg envConfig, err error) error {
	var aggregate env.AggregateError
	if !errors.As(err, &aggregate) {
		return err
	}
	keys := envKeysByField(reflect.TypeOf(cfg).Elem())
	msgs := make([]string, 0, len(aggregate.Errors))
	for _, one := range aggregate.Errors {
		var parseErr env.ParseError
		if errors.As(one, &parseErr) {
			if key, ok := keys[parseErr.Name]; ok {
				msgs = append(msgs, fmt.Sprintf("%s: %v", key, parseErr.Err))
				continue
			}
		}
		msgs = append(msgs, one.Error())
	}
	return errors.New(strings.Join(msgs, "; "))
}

// envKeysByField maps the Go field names of a config struct, and of the config
// structs nested in it, to the environment variables they read.
func envKeysByField(structType reflect.Type) map[string]string {
	keys := make(map[string]string, structType.NumField())
	for field := range structType.Fields() {
		if field.Type.Kind() == reflect.Struct {
			maps.Copy(keys, envKeysByField(field.Type))
			continue
		}
		if tag := field.Tag.Get(envTag); tag != "" {
			keys[field.Name] = strings.Split(tag, ",")[0]
		}
	}
	return keys
}

// envInt is an integer value together with the variable it was read from.
type envInt struct {
	name  string
	value int
}

// requirePositiveInts rejects values below one.
func requirePositiveInts(ints ...envInt) error {
	for _, i := range ints {
		if i.value < 1 {
			return fmt.Errorf("%s must be positive, got %d", i.name, i.value)
		}
	}
	return nil
}

// newResultPoller loads the AWS config for cfg.Region and builds the S3 client
// and SSM runner the poller and its diagnostics use.
func newResultPoller(ctx context.Context, cfg pollerConfig) (*resultPoller, error) {
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
		interval:        cfg.pollInterval(),
		debugLogLines:   cfg.DebugLogLines,
		debugEveryPolls: cfg.DebugLogEveryPolls,
	}, nil
}
