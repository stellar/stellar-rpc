package harness

import (
	"context"
	"errors"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const resultFetchTimeout = 30 * time.Second

// resultPoller waits for a result with an exact run-attempt match. An absent
// key is a healthy wait: S3 reports a missing key as 404 and a permissions fault as 403.
type resultPoller struct {
	s3Client        *s3.Client
	runner          *ssmRunner
	bucket, key     string
	runID           string
	interval        time.Duration
	debugLogLines   int
	debugEveryPolls int
}

// poll waits for a result within one job's time budget. Window expiry returns
// (nil, nil) so Gather can report a timeout and Relay can hand off to another
// job if campaign time remains.
func (p *resultPoller) poll(ctx context.Context, until time.Time) (*Result, error) {
	windowCtx, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	for pollCount := 1; time.Now().Before(until); pollCount++ {
		res, err := p.checkOnce(windowCtx)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if windowCtx.Err() != nil {
			break
		}
		switch {
		case isPermanentFetchError(err):
			return nil, err
		case err != nil:
			logger.Warnf("result fetch failed; retrying: %v", err)
		case res != nil:
			return res, nil
		}

		if pollCount%p.debugEveryPolls == 0 {
			logger.Infof("debug tail:\n%s", p.runner.debugTail(windowCtx, p.debugLogLines))
		}
		if left := time.Until(until); left > 0 {
			if err := sleepContext(windowCtx, min(p.interval, left)); err != nil {
				break
			}
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// No verdict and no fault: the caller decides what a closed window means.
	return nil, nil //nolint:nilnil
}

// checkOnce fetches the key once and classifies what it finds. It returns a
// result only for a verdict from this run. An absent key or a result left by
// another attempt is a healthy wait (nil, nil); everything else is an error.
func (p *resultPoller) checkOnce(ctx context.Context) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, resultFetchTimeout)
	defer cancel()
	res, err := FetchResult(ctx, p.s3Client, p.bucket, p.key)
	switch {
	case errors.Is(err, ErrResultNotReady):
		logger.Infof("still waiting for s3://%s/%s", p.bucket, p.key)
		return nil, nil //nolint:nilnil // absent is a healthy wait
	case err != nil:
		return nil, err
	// Re-run attempts share RESULT_KEY; this attempt's box overwrites a
	// predecessor's result when it finishes.
	case res.RunID != p.runID:
		logger.Infof("ignoring stale result from run %q (want %q)", res.RunID, p.runID)
		return nil, nil //nolint:nilnil // stale is a healthy wait
	default:
		return res, nil
	}
}

// isPermanentFetchError reports whether a fetch error cannot heal on its own:
// an object that violates the result protocol, or an S3 rejection of the
// request as sent (AccessDenied, NoSuchBucket, an expired session). Transport
// failures and 5xx responses are left to the retry loop.
func isPermanentFetchError(err error) bool {
	if errors.Is(err, ErrInvalidResult) {
		return true
	}
	var re *awshttp.ResponseError
	if !errors.As(err, &re) {
		return false // no response at all: a transport failure
	}
	if code := re.HTTPStatusCode(); code < 400 || code >= 500 {
		return false
	}
	// S3 answers an idle socket with 400 RequestTimeout; it heals on retry.
	var apiErr smithy.APIError
	return !errors.As(err, &apiErr) || apiErr.ErrorCode() != "RequestTimeout"
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
