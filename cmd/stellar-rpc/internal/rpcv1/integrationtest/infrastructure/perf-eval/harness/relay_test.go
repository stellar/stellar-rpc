package harness

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/require"
)

// relayEnv is the poller env contract, valid values throughout. Tests blank or
// corrupt one slot at a time from it.
var relayEnv = map[string]string{
	"INSTANCE_ID":           "i-0123456789abcdef0",
	"AWS_REGION":            "us-east-1",
	"GITHUB_OUTPUT":         "/dev/null",
	"BUCKET":                "stellar-rpc-ci-load-test",
	"RESULT_KEY":            "runs/1/campaign/result.json",
	"RUN_ID":                "1-1",
	"POLL_INTERVAL":         "30",
	"DEBUG_LOG_LINES":       "40",
	"DEBUG_LOG_EVERY_POLLS": "10",
	"WINDOW_SECONDS":        "19200",
	"DEADLINE_EPOCH":        "1700000000",
}

// setRelayEnv installs the contract with overrides applied; an empty override
// value stands for an unset variable.
func setRelayEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	for k, v := range relayEnv {
		if o, ok := overrides[k]; ok {
			v = o
		}
		t.Setenv(k, v)
	}
}

// TestRelayEnvValidation checks that a mis-plumbed workflow gets an error naming
// the bad slot rather than a panic or an AWS call. Every case here must fail
// before Relay reaches S3, which is why the env is otherwise complete.
func TestRelayEnvValidation(t *testing.T) {
	blankAll := map[string]string{}
	for k := range relayEnv {
		blankAll[k] = ""
	}
	for _, tc := range []struct {
		name      string
		overrides map[string]string
		wantMsg   string
	}{
		{"nothing set", blankAll, "INSTANCE_ID"},
		{"no instance", map[string]string{"INSTANCE_ID": ""}, "INSTANCE_ID"},
		{"no deadline", map[string]string{"DEADLINE_EPOCH": ""}, "DEADLINE_EPOCH"},
		{"unparsable interval", map[string]string{"POLL_INTERVAL": "half a minute"}, "POLL_INTERVAL"},
		{"zero debug cadence", map[string]string{"DEBUG_LOG_EVERY_POLLS": "0"}, "DEBUG_LOG_EVERY_POLLS"},
		{"zero poll interval", map[string]string{"POLL_INTERVAL": "0"}, "POLL_INTERVAL"},
		{"negative window", map[string]string{"WINDOW_SECONDS": "-1"}, "WINDOW_SECONDS"},
		{"integer overflow", map[string]string{"POLL_INTERVAL": "99999999999999999999"}, "POLL_INTERVAL"},
		{"negative debug lines", map[string]string{"DEBUG_LOG_LINES": "-1"}, "DEBUG_LOG_LINES"},
		{"zero deadline", map[string]string{"DEADLINE_EPOCH": "0"}, "DEADLINE_EPOCH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRelayEnv(t, tc.overrides)
			err := Relay(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

func TestRelayWindowDeadline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline time.Duration
		state    string
		calls    int
	}{
		{"budget left", time.Hour, "running", 2},
		{"one second left", time.Minute + time.Second, "running", 2},
		{"deadline reached", time.Minute, "fail", 3},
		{"deadline passed", -time.Hour, "fail", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dir := t.TempDir()
				t.Setenv("RESULTS_FILE", filepath.Join(dir, "results.md"))
				output := filepath.Join(dir, "outputs")
				calls := 0
				p := testPoller(func(*http.Request) (*http.Response, error) {
					calls++
					return noSuchKey()
				})
				p.runner = testDebugRunner()
				start := time.Now()
				r := &relay{poller: p, githubOutput: output, window: time.Minute, deadline: start.Add(tc.deadline)}
				require.NoError(t, r.poll(t.Context()))
				data, err := os.ReadFile(output)
				require.NoError(t, err)
				require.Equal(t, "state="+tc.state+"\n", string(data))
				require.Equal(t, tc.calls, calls)
				if tc.state == "running" {
					require.Equal(t, time.Minute, time.Since(start))
					require.NoFileExists(t, filepath.Join(dir, "timeout-comment.md"))
				} else {
					data, err = os.ReadFile(filepath.Join(dir, "timeout-comment.md"))
					require.NoError(t, err)
					require.Contains(t, string(data), "budget deadline passed with no verdict")
				}
			})
		})
	}
}

func TestGatherEnvValidation(t *testing.T) {
	for _, key := range []string{"RESULTS_TIMEOUT", "POLL_INTERVAL", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS"} {
		for _, value := range []string{"", "0", "-1", "invalid", "99999999999999999999"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				setRelayEnv(t, nil)
				t.Setenv("RESULTS_TIMEOUT", "300")
				t.Setenv(key, value)
				require.ErrorContains(t, Gather(t.Context()), key)
			})
		}
	}
}

func TestRelayFinalFetch(t *testing.T) {
	for _, when := range []string{"during final sleep", "already expired"} {
		t.Run(when, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				output := filepath.Join(t.TempDir(), "outputs")
				results := filepath.Join(t.TempDir(), "results.md")
				t.Setenv("RESULTS_FILE", results)
				start := time.Now()
				deadline := start.Add(time.Second)
				if when == "already expired" {
					deadline = start.Add(-time.Hour)
				}
				calls := 0
				p := testPoller(func(*http.Request) (*http.Response, error) {
					calls++
					if time.Now().Before(deadline) {
						return noSuchKey()
					}
					return resultResponse(200, resultJSON("1-2", "ok"))
				})
				r := &relay{poller: p, githubOutput: output, window: time.Hour, deadline: deadline}
				require.NoError(t, r.poll(t.Context()))
				data, err := os.ReadFile(output)
				require.NoError(t, err)
				require.Equal(t, "state=ok\n", string(data))
				data, err = os.ReadFile(results)
				require.NoError(t, err)
				require.Equal(t, "report", string(data))
				if when == "already expired" {
					require.Equal(t, 1, calls)
				} else {
					require.Equal(t, 2, calls)
				}
			})
		})
	}
}

func TestGatherFinalOutputs(t *testing.T) {
	for _, verdict := range []string{"ok", "fail"} {
		t.Run(verdict, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "outputs")
			results := filepath.Join(t.TempDir(), "results.md")
			t.Setenv("RESULTS_FILE", results)
			res := &Result{Verdict: verdict, Markdown: "report"}
			require.NoError(t, reportGather(t.Context(), &resultPoller{}, output, time.Minute, res, nil))
			data, err := os.ReadFile(output)
			require.NoError(t, err)
			passed := "false"
			if verdict == "ok" {
				passed = "true"
			}
			require.Equal(t, "found=true\npassed="+passed+"\n", string(data))
			data, err = os.ReadFile(results)
			require.NoError(t, err)
			require.Equal(t, res.Markdown, string(data))
		})
	}
}

func testDebugRunner() *ssmRunner {
	return &ssmRunner{instanceID: "i-test", client: ssm.New(ssm.Options{
		Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1,
		HTTPClient: &http.Client{Transport: resultTransport(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("SSM unavailable")
		})},
	})}
}

func TestRelayNoVerdict(t *testing.T) {
	for _, kind := range []string{"missing", "stale", "denied", "blocked final fetch"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dir := t.TempDir()
				t.Setenv("RESULTS_FILE", filepath.Join(dir, "results.md"))
				output := filepath.Join(dir, "outputs")
				calls := 0
				p := testPoller(func(r *http.Request) (*http.Response, error) {
					calls++
					switch kind {
					case "stale":
						return resultResponse(200, resultJSON("1-1", "ok"))
					case "denied":
						return accessDenied()
					case "blocked final fetch":
						<-r.Context().Done()
						return nil, r.Context().Err()
					default:
						return noSuchKey()
					}
				})
				p.runner = testDebugRunner()
				p.debugLogLines = 40
				start := time.Now()
				r := &relay{poller: p, githubOutput: output, window: time.Hour, deadline: start}
				require.NoError(t, r.poll(t.Context()))
				require.Equal(t, 1, calls)
				data, err := os.ReadFile(output)
				require.NoError(t, err)
				require.Equal(t, "state=fail\n", string(data))
				data, err = os.ReadFile(filepath.Join(dir, "timeout-comment.md"))
				require.NoError(t, err)
				if kind == "missing" || kind == "stale" {
					require.Contains(t, string(data), "budget deadline passed with no verdict")
				} else {
					require.Contains(t, string(data), "final result fetch failed")
				}
				require.NoFileExists(t, filepath.Join(dir, "results.md"))
				require.LessOrEqual(t, time.Since(start), resultFetchTimeout+commandWaitTimeout)
			})
		})
	}
}

func TestGatherNoVerdictOutputs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv("RESULTS_FILE", filepath.Join(dir, "results.md"))
		output := filepath.Join(dir, "outputs")
		p := &resultPoller{runner: testDebugRunner(), debugLogLines: 40}
		require.NoError(t, reportGather(t.Context(), p, output, time.Minute, nil, errors.New("polling failed")))
		data, err := os.ReadFile(output)
		require.NoError(t, err)
		require.Equal(t, "found=false\n", string(data))
		data, err = os.ReadFile(filepath.Join(dir, "timeout-comment.md"))
		require.NoError(t, err)
		require.Contains(t, string(data), "polling failed")
		require.NoFileExists(t, filepath.Join(dir, "results.md"))
	})
}

func TestReportCancellationDuringDiagnostics(t *testing.T) {
	for _, caller := range []string{"gather", "relay"} {
		t.Run(caller, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dir := t.TempDir()
				t.Setenv("RESULTS_FILE", filepath.Join(dir, "results.md"))
				output := filepath.Join(dir, "outputs")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				runner := &ssmRunner{instanceID: "i-test", client: ssm.New(ssm.Options{
					Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1,
					HTTPClient: &http.Client{Transport: resultTransport(func(r *http.Request) (*http.Response, error) {
						time.AfterFunc(time.Second, cancel)
						<-r.Context().Done()
						return nil, r.Context().Err()
					})},
				})}
				start := time.Now()
				p := &resultPoller{runner: runner, debugLogLines: 40}
				var err error
				if caller == "gather" {
					err = reportGather(ctx, p, output, time.Minute, nil, nil)
				} else {
					r := &relay{poller: p, githubOutput: output}
					err = r.reportFault(ctx, "polling failed")
				}
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, time.Second, time.Since(start))
				require.NoFileExists(t, output)
				require.NoFileExists(t, filepath.Join(dir, "results.md"))
				require.NoFileExists(t, filepath.Join(dir, "timeout-comment.md"))
			})
		})
	}
}

func TestSSMDiagnosticBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runner := testDebugRunner()
		runner.client = ssm.New(ssm.Options{
			Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1,
			HTTPClient: &http.Client{Transport: resultTransport(func(r *http.Request) (*http.Response, error) {
				<-r.Context().Done()
				return nil, r.Context().Err()
			})},
		})
		start := time.Now()
		require.Equal(t, "__DEBUG_TAIL_UNAVAILABLE__", runner.debugTail(t.Context(), 40))
		require.Equal(t, commandWaitTimeout, time.Since(start))
	})
}

func TestRelayCancellation(t *testing.T) {
	for _, expired := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("RESULTS_FILE", filepath.Join(dir, "results.md"))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := testPoller(func(r *http.Request) (*http.Response, error) {
				time.AfterFunc(time.Second, cancel)
				<-r.Context().Done()
				return nil, r.Context().Err()
			})
			deadline := time.Now().Add(time.Hour)
			if expired {
				deadline = time.Now()
			}
			output := filepath.Join(dir, "outputs")
			r := &relay{poller: p, githubOutput: output, window: time.Minute, deadline: deadline}
			require.ErrorIs(t, r.poll(ctx), context.Canceled)
			require.NoFileExists(t, output)
			require.NoFileExists(t, filepath.Join(dir, "results.md"))
			require.NoFileExists(t, filepath.Join(dir, "timeout-comment.md"))
		})
	}
}
