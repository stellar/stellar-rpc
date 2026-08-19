package harness

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Ten failed polls in a row, about 5 min at the 30 s interval. Persistent
// errors are a permissions or config fault, not a slow campaign.
const maxConsecutiveFetchErrors = 10

// resultPoller polls one S3 result key until a final verdict for its run
// appears or the window closes. Gather and Relay share it; they differ only in
// the window they poll and in what they report afterwards.
type resultPoller struct {
	s3Client    *s3.Client
	runner      *ssmRunner
	bucket, key string
	runID       string
	interval    time.Duration
	// keySeeded marks a key the launch job pre-writes with a pending marker, so
	// a 404 is a fault (a silent seed failure or a mistyped key) rather than a
	// campaign that has not published yet. Legs publish only at the end, so
	// their gatherer leaves this false and 404s stay normal.
	keySeeded       bool
	debugLogLines   int
	debugEveryPolls int
}

// poll polls until `until`. It returns (res, nil) when a final verdict for
// this run appears, (nil, nil) when the window closes without one, and an
// error after maxConsecutiveFetchErrors consecutive failed fetches.
func (p *resultPoller) poll(ctx context.Context, until time.Time) (*Result, error) {
	fetchErrs := 0
	for pollCount := 1; time.Now().Before(until); pollCount++ {
		res, err := p.checkOnce(ctx)
		switch {
		// checkOnce already logged the wait.
		case errors.Is(err, ErrResultNotReady):
			if p.keySeeded {
				fetchErrs++ // a seeded key that 404s is a fault, not a wait
			} else {
				fetchErrs = 0 // legs publish at the end, so a 404 is a healthy answer
			}
		case err != nil:
			fetchErrs++
			logger.Warnf("result fetch failed (%d/%d); retrying: %v", fetchErrs, maxConsecutiveFetchErrors, err)
		case res != nil:
			return res, nil
		default:
			fetchErrs = 0 // a stale or pending result is still a healthy fetch
		}
		if fetchErrs >= maxConsecutiveFetchErrors {
			return nil, p.giveUpErr(fetchErrs, err)
		}

		if pollCount%p.debugEveryPolls == 0 {
			logger.Infof("debug tail:\n%s", p.runner.debugTail(ctx, p.debugLogLines))
		}
		// Never sleep past the window; the loop only rechecks at the top. The
		// in-flight calls above can still overrun `until` by one fetch plus one
		// debug tail, which the window sizing carries as margin: the AWS calls
		// keep the caller's context rather than a deadline of their own.
		if left := time.Until(until); left > 0 {
			time.Sleep(min(p.interval, left))
		}
	}
	// No verdict and no fault: the caller decides what a closed window means.
	return nil, nil //nolint:nilnil
}

// checkOnce fetches the key once and classifies what it finds. It returns a
// result only for a final verdict from this run; a stale result or the pending
// marker returns (nil, nil), and every fetch failure (404 included) comes back
// as an error so the caller can count it.
func (p *resultPoller) checkOnce(ctx context.Context) (*Result, error) {
	res, err := FetchResult(ctx, p.s3Client, p.bucket, p.key)
	switch {
	case errors.Is(err, ErrResultNotReady):
		logger.Infof("still waiting for s3://%s/%s", p.bucket, p.key)
		return nil, err
	case err != nil:
		return nil, err
	// Re-run attempts share RESULT_KEY, so skip results with a stale RunID.
	case res.RunID != p.runID:
		logger.Infof("ignoring stale result from run %s (want %s)", res.RunID, p.runID)
		return nil, nil //nolint:nilnil // a healthy fetch with nothing final to report
	case res.Verdict == VerdictPending:
		logger.Infof("campaign still running (pending marker at s3://%s/%s)", p.bucket, p.key)
		return nil, nil //nolint:nilnil // same: keep polling
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
				"The launch job seeds this key, so this is a seeding or config fault, not a pending campaign",
			p.bucket, p.key, fetchErrs)
	}
	return fmt.Errorf(
		"❌ Gave up: %d consecutive result fetches failed (last: %v). "+
			"This is a permissions or config fault, not a pending campaign",
		fetchErrs, last)
}
