package harness

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Ten failed polls in a row, about 5 min at the 30 s interval.
const maxConsecutiveFetchErrors = 10

const resultFetchTimeout = 30 * time.Second

// resultPoller waits for a final result with an exact run-attempt match.
// Its workflow must seed the result key before launching the producer.
type resultPoller struct {
	s3Client        *s3.Client
	runner          *ssmRunner
	bucket, key     string
	runID           string
	interval        time.Duration
	debugLogLines   int
	debugEveryPolls int
}

// poll polls until `until`. It returns (res, nil) when a final verdict for
// this run appears, (nil, nil) when the window closes without one, and an
// error on cancellation, invalid data, or ten consecutive failed polls.
// Each window starts a new error count; a current pending marker resets it.
func (p *resultPoller) poll(ctx context.Context, until time.Time) (*Result, error) {
	windowCtx, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	fetchErrs := 0
	for pollCount := 1; time.Now().Before(until); pollCount++ {
		res, err := p.checkOnce(windowCtx)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if windowCtx.Err() != nil {
			break
		}
		switch {
		case errors.Is(err, ErrInvalidResult):
			return nil, err
		case err != nil:
			fetchErrs++
			logger.Warnf("result fetch failed (%d/%d); retrying: %v", fetchErrs, maxConsecutiveFetchErrors, err)
		case res != nil:
			return res, nil
		default:
			fetchErrs = 0 // only a current pending marker is a healthy wait
		}
		if fetchErrs >= maxConsecutiveFetchErrors {
			return nil, p.giveUpErr(fetchErrs, err)
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
// result only for a final verdict from this run. A current pending marker is
// a healthy wait; stale results and fetch failures count toward the error limit.
func (p *resultPoller) checkOnce(ctx context.Context) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, resultFetchTimeout)
	defer cancel()
	res, err := FetchResult(ctx, p.s3Client, p.bucket, p.key)
	switch {
	case errors.Is(err, ErrResultNotReady):
		logger.Infof("still waiting for s3://%s/%s", p.bucket, p.key)
		return nil, err
	case err != nil:
		return nil, err
	// Re-run attempts share RESULT_KEY, so skip results with a stale RunID.
	case res.RunID != p.runID:
		return nil, fmt.Errorf("stale result from run %q (want %q)", res.RunID, p.runID)
	case res.Verdict == VerdictPending:
		logger.Infof("campaign still running (pending marker at s3://%s/%s)", p.bucket, p.key)
		return nil, nil //nolint:nilnil // pending is a healthy wait
	default:
		return res, nil
	}
}

// giveUpErr is the headline for a run of failed fetches, phrased for whichever
// failure ended the run.
func (p *resultPoller) giveUpErr(fetchErrs int, last error) error {
	if errors.Is(last, ErrResultNotReady) {
		return fmt.Errorf(
			"❌ Gave up: s3://%s/%s was absent on %d consecutive polls. "+
				"The workflow seeds this key, so this is a seeding or config fault, not a pending campaign",
			p.bucket, p.key, fetchErrs)
	}
	return fmt.Errorf(
		"gave up: %d consecutive result polls failed (last: %v). "+
			"Check result seeding, run identity, S3 permissions, and transport availability",
		fetchErrs, last)
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
