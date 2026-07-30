// Command coordinator-runner renders the release perf-eval coordinator's sticky
// PR comment. It reads the prior comment body on stdin, fetches each leg's result
// object from S3, and writes the new comment body to stdout: the current run as
// "Performance Evaluation Test #N" on top, with prior runs folded into drop-downs.
//
// The run history is data that rides inside the comment as a base64 JSON blob in an
// HTML comment. Each render decodes it, prepends the current run, and re-renders
// the whole body from it.
//
// The coordinator workflow (load-test-coordinator.yml) owns the GitHub work: it
// reads the existing comment, pipes it here, and posts/edits what this prints.
package main

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/harness"
)

const (
	marker = "<!-- load-test-results -->" // keys the sticky comment so the workflow can find it

	maxHistory    = 10    // runs kept in the history (and so rendered as drop-downs)
	maxCommentLen = 60000 // shed oldest runs beyond this; GitHub caps comments at 65536
)

var (
	logger    = supportlog.New()
	historyRe = regexp.MustCompile(`^<!-- perf-eval-history: ([A-Za-z0-9+/=]+) -->$`)
)

// leg is one entry of the LEGS env: where to fetch a leg's result object.
type leg struct {
	Label  string `json:"label"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// legResult is one leg's outcome as recorded in the history blob.
type legResult struct {
	Label    string `json:"label"`
	Verdict  string `json:"verdict"` // "ok"/"fail", empty when no result was published
	Markdown string `json:"markdown"`
}

// runRecord is one coordinator run in the history blob, newest first.
type runRecord struct {
	Num       int         `json:"num"`
	TargetSHA string      `json:"targetSha"`
	TargetRef string      `json:"targetRef"`
	RunURL    string      `json:"runUrl"`
	Legs      []legResult `json:"legs"`
}

func main() {
	logger.SetLevel(supportlog.InfoLevel)
	if err := run(context.Background()); err != nil {
		logger.Errorf("fatal: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	legs, err := parseLegs(os.Getenv("LEGS"))
	if err != nil {
		return fmt.Errorf("parsing LEGS: %w", err)
	}
	prev, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("reading previous comment from stdin: %w", err)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cmp.Or(os.Getenv("AWS_REGION"), "us-east-1")))
	if err != nil {
		return err
	}
	// A missing/unreadable result object (timeout or pre-publish failure) renders
	// as a failed leg rather than aborting the report.
	s3Client := s3.NewFromConfig(awsCfg)
	results := make([]legResult, len(legs))
	for i, l := range legs {
		results[i] = legResult{Label: l.Label}
		res, err := harness.FetchResult(ctx, s3Client, l.Bucket, l.Key)
		if err != nil {
			logger.Warnf("no result at s3://%s/%s: %v", l.Bucket, l.Key, err)
			continue
		}
		results[i].Verdict, results[i].Markdown = res.Verdict, res.Markdown
	}

	body := renderComment(runRecord{
		TargetSHA: os.Getenv("TARGET_SHA"),
		TargetRef: os.Getenv("TARGET_REF"),
		RunURL:    os.Getenv("RUN_URL"),
		Legs:      results,
	}, string(prev))
	_, err = io.WriteString(os.Stdout, body)
	return err
}

// renderComment numbers cur, prepends it to the prior comment's history, and
// renders the whole body from it, shedding the oldest runs to stay within the
// history and comment-size caps.
func renderComment(cur runRecord, prev string) string {
	hist := parseHistory(prev)
	cur.Num = 1
	if len(hist) > 0 {
		cur.Num = hist[0].Num + 1
	}
	hist = append([]runRecord{cur}, hist...)
	if len(hist) > maxHistory {
		hist = hist[:maxHistory]
	}
	body := renderBody(hist)
	for len(body) > maxCommentLen && len(hist) > 1 {
		hist = hist[:len(hist)-1]
		body = renderBody(hist)
	}
	return body
}

func renderBody(hist []runRecord) string {
	var b strings.Builder
	fmt.Fprintln(&b, marker)
	fmt.Fprintf(&b, "<!-- perf-eval-history: %s -->\n", encodeHistory(hist))
	b.WriteString(renderRun(hist[0]))
	for _, r := range hist[1:] {
		fmt.Fprintf(&b, "\n<details>\n<summary>Performance Evaluation Test #%d</summary>\n\n", r.Num)
		b.WriteString(strings.Trim(renderRun(r), "\n"))
		b.WriteString("\n</details>\n")
	}
	return b.String()
}

func renderRun(r runRecord) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## 🧪 Performance Evaluation Test #%d\n\n", r.Num)
	fmt.Fprintf(&b, "**Commit:** `%s` (`%s`)\n", r.TargetSHA[:min(12, len(r.TargetSHA))], r.TargetRef)
	fmt.Fprintf(&b, "**Run:** %s\n", r.RunURL)
	for _, l := range r.Legs {
		b.WriteString(renderLeg(l))
	}
	return b.String()
}

// renderLeg renders one leg's section: a verdict heading plus its result markdown,
// or a fallback when no result object was published.
func renderLeg(l legResult) string {
	emoji := "❌"
	if l.Verdict == "ok" {
		emoji = "✅"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n### %s %s — verdict: %s\n\n", emoji, l.Label, cmp.Or(l.Verdict, "none"))
	if strings.TrimSpace(l.Markdown) != "" {
		b.WriteString(strings.TrimRight(l.Markdown, "\n"))
		b.WriteByte('\n')
	} else {
		b.WriteString("_No result object published (leg timed out or failed before publishing). See the run logs._\n")
	}
	return b.String()
}

func encodeHistory(hist []runRecord) string {
	data, err := json.Marshal(hist)
	if err != nil {
		logger.Warnf("marshaling history; next run will start fresh: %v", err)
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// parseHistory recovers the run history from the blob line of a prior comment
// body. An absent or undecodable blob starts the history fresh.
func parseHistory(prev string) []runRecord {
	for line := range strings.SplitSeq(prev, "\n") {
		m := historyRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(m[1])
		if err != nil {
			logger.Warnf("undecodable history blob; starting fresh: %v", err)
			return nil
		}
		var hist []runRecord
		if err := json.Unmarshal(data, &hist); err != nil {
			logger.Warnf("unparseable history blob; starting fresh: %v", err)
			return nil
		}
		return hist
	}
	return nil
}

func parseLegs(s string) ([]leg, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var legs []leg
	if err := json.Unmarshal([]byte(s), &legs); err != nil {
		return nil, err
	}
	return legs, nil
}
