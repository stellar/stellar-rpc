package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type resultTransport func(*http.Request) (*http.Response, error)

func (f resultTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testPoller(transport resultTransport) *resultPoller {
	return &resultPoller{
		s3Client: s3.New(s3.Options{
			Region: "us-east-1", Credentials: aws.AnonymousCredentials{},
			HTTPClient: &http.Client{Transport: transport}, RetryMaxAttempts: 1,
		}),
		bucket: "test-bucket", key: "runs/1/result.json", runID: "1-2",
		interval: 30 * time.Second, debugEveryPolls: 1000,
	}
}

func resultResponse(status int, body string) (*http.Response, error) {
	return &http.Response{
		StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)),
	}, nil
}

func resultJSON(run, verdict string) string {
	return fmt.Sprintf(`{"schemaVersion":1,"runId":%q,"verdict":%q,"markdown":"report"}`, run, verdict)
}

func TestCheckOnce(t *testing.T) {
	for _, tc := range []struct {
		name, body, verdict string
		status              int
		wantErr             string
	}{
		{name: "ok", body: resultJSON("1-2", "ok"), verdict: "ok", status: 200},
		{name: "fail", body: resultJSON("1-2", "fail"), verdict: "fail", status: 200},
		{name: "pending", body: resultJSON("1-2", "pending"), status: 200},
		{name: "prior attempt", body: resultJSON("1-1", "ok"), status: 200, wantErr: "stale result"},
		{name: "other run", body: resultJSON("2-2", "fail"), status: 200, wantErr: "stale result"},
		{name: "missing", body: `<Error><Code>NoSuchKey</Code></Error>`, status: 404, wantErr: "not published"},
		{name: "inaccessible", body: `<Error><Code>AccessDenied</Code></Error>`, status: 403, wantErr: "AccessDenied"},
		{name: "malformed", body: `{`, status: 200, wantErr: "invalid result"},
		{name: "null", body: `null`, status: 200, wantErr: "invalid result"},
		{name: "unknown verdict", body: resultJSON("1-2", "success"), status: 200, wantErr: "invalid result"},
		{name: "no identity", body: resultJSON("", "ok"), status: 200, wantErr: "invalid result"},
		{
			name: "unknown schema", body: `{"schemaVersion":2,"runId":"1-2","verdict":"ok"}`,
			status: 200, wantErr: "invalid result",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testPoller(func(*http.Request) (*http.Response, error) { return resultResponse(tc.status, tc.body) })
			res, err := p.checkOnce(t.Context())
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, res)
				return
			}
			require.NoError(t, err)
			if tc.verdict == "" {
				require.Nil(t, res)
			} else {
				require.NotNil(t, res)
				require.Equal(t, tc.verdict, res.Verdict)
			}
		})
	}
}

func TestPollErrorThreshold(t *testing.T) {
	for _, kind := range []string{"missing", "denied", "stale", "transport", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				p := testPoller(func(*http.Request) (*http.Response, error) {
					calls++
					switch kind {
					case "missing":
						return resultResponse(404, `<Error><Code>NoSuchKey</Code></Error>`)
					case "denied":
						return resultResponse(403, `<Error><Code>AccessDenied</Code></Error>`)
					case "stale":
						return resultResponse(200, resultJSON("1-1", "ok"))
					case "malformed":
						return resultResponse(200, `{`)
					default:
						return nil, errors.New("temporary transport failure")
					}
				})
				start := time.Now()
				res, err := p.poll(t.Context(), start.Add(time.Hour))
				require.Error(t, err)
				require.Nil(t, res)
				if kind == "malformed" {
					require.ErrorIs(t, err, ErrInvalidResult)
					require.Equal(t, 1, calls)
				} else {
					require.Equal(t, maxConsecutiveFetchErrors, calls)
					require.Equal(t, 9*p.interval, time.Since(start))
				}
			})
		})
	}
}

func TestPollPendingResetsErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		p := testPoller(func(*http.Request) (*http.Response, error) {
			calls++
			switch calls {
			case 10:
				return resultResponse(200, resultJSON("1-2", "pending"))
			case 20:
				return resultResponse(200, resultJSON("1-2", "ok"))
			default:
				return nil, errors.New("temporary transport failure")
			}
		})
		res, err := p.poll(t.Context(), time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.Equal(t, "ok", res.Verdict)
		require.Equal(t, 20, calls)
	})
}

func TestPollWindowAndHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		p := testPoller(func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 4 {
				return resultResponse(200, resultJSON("1-2", "fail"))
			}
			return resultResponse(200, resultJSON("1-2", "pending"))
		})
		start := time.Now()
		res, err := p.poll(t.Context(), start.Add(65*time.Second))
		require.NoError(t, err)
		require.Nil(t, res)
		require.Equal(t, 3, calls)
		require.Equal(t, 65*time.Second, time.Since(start))
		res, err = p.poll(t.Context(), time.Now().Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, "fail", res.Verdict)
		require.Equal(t, 4, calls)
	})
}

type blockedBody struct{ read func() error }

func (b blockedBody) Read([]byte) (int, error) { return 0, b.read() }
func (blockedBody) Close() error               { return nil }

func TestPollCancellationAndBounds(t *testing.T) {
	const (
		fetchTimeout = "fetch timeout"
		window       = "window"
	)
	for _, phase := range []string{"before poll", "sleep", "request", "body", window, fetchTimeout} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				calls := 0
				p := testPoller(func(r *http.Request) (*http.Response, error) {
					calls++
					if phase == "request" || phase == window || phase == fetchTimeout {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					if phase == "body" {
						body := blockedBody{func() error { <-r.Context().Done(); return r.Context().Err() }}
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}, nil
					}
					return resultResponse(200, resultJSON("1-2", "pending"))
				})
				start := time.Now()
				if phase == "before poll" {
					cancel()
				} else if phase != window && phase != fetchTimeout {
					time.AfterFunc(time.Second, cancel)
				}
				until := start.Add(5 * time.Second)
				if phase == fetchTimeout {
					until = start.Add(35 * time.Second)
				}
				res, err := p.poll(ctx, until)
				require.Nil(t, res)
				if phase == window || phase == fetchTimeout {
					require.NoError(t, err)
					require.Equal(t, until, time.Now())
				} else {
					require.ErrorIs(t, err, context.Canceled)
					require.LessOrEqual(t, time.Since(start), time.Second)
				}
				require.LessOrEqual(t, calls, 1)
			})
		})
	}
}
