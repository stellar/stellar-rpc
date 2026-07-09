// Command coordinator-runner renders the release perf-eval coordinator's sticky
// PR comment. It reads the prior comment body on stdin, fetches each leg's result
// object from S3, and writes the new comment body to stdout: the current run as
// "Performance Evaluation Test #N" on top, with prior runs folded into drop-downs.
//
// The coordinator workflow (load-test-coordinator.yml) owns the GitHub work: it
// reads the existing comment, pipes it here, and posts/edits what this prints.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

const marker = "<!-- load-test-results -->" // keys the sticky comment so the workflow can find it

var (
	logger          = supportlog.New()
	perfEvalRe      = regexp.MustCompile(`^<!-- perf-eval: ([0-9]+) -->$`)
	seriesHeadingRe = regexp.MustCompile(`^## .*Performance Evaluation Test #[0-9]+$`)
)

// leg is one perf-eval area from the coordinator workflow; verdict and markdown
// come from its result object at s3://Bucket/Key (both "" when none published).
type leg struct {
	Label  string `json:"label"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`

	verdict  string // "ok"/"fail", empty on timeout
	markdown string
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
	s3Client := s3.NewFromConfig(awsCfg)
	for i := range legs {
		legs[i].verdict, legs[i].markdown = fetchResult(ctx, s3Client, legs[i].Bucket, legs[i].Key)
	}

	body := renderComment(commentInput{
		targetSHA: os.Getenv("TARGET_SHA"),
		targetRef: os.Getenv("TARGET_REF"),
		runURL:    os.Getenv("RUN_URL"),
		legs:      legs,
		prev:      string(prev),
	})
	_, err = io.WriteString(os.Stdout, body)
	return err
}

type commentInput struct {
	targetSHA string
	targetRef string
	runURL    string
	legs      []leg
	prev      string
}

// renderComment builds the new sticky-comment body: the current run as test #N on
// top, with the prior comment folded into sibling #N-1 / #N-2 ... drop-downs.
func renderComment(in commentInput) string {
	prevNum := parsePrevNumber(in.prev)

	var b strings.Builder
	fmt.Fprintln(&b, marker)
	fmt.Fprintf(&b, "<!-- perf-eval: %d -->\n", prevNum+1)
	fmt.Fprintf(&b, "## 🧪 Performance Evaluation Test #%d\n\n", prevNum+1)
	fmt.Fprintf(&b, "**Commit:** `%s` (`%s`)\n", in.targetSHA[:min(12, len(in.targetSHA))], in.targetRef)
	fmt.Fprintf(&b, "**Run:** %s\n", in.runURL)

	for _, l := range in.legs {
		b.WriteString(renderLeg(l))
	}
	b.WriteString(foldPrevious(in.prev, prevNum))
	return b.String()
}

// renderLeg renders one leg's section: a verdict heading plus its result markdown,
// or a fallback when no result object was published.
func renderLeg(l leg) string {
	emoji := "❌"
	if l.verdict == "ok" {
		emoji = "✅"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n### %s %s — verdict: %s\n\n", emoji, l.Label, cmp.Or(l.verdict, "none"))
	if strings.TrimSpace(l.markdown) != "" {
		b.WriteString(strings.TrimRight(l.markdown, "\n"))
		b.WriteByte('\n')
	} else {
		b.WriteString("_No result object published (leg timed out or failed before publishing). See the run logs._\n")
	}
	return b.String()
}

// foldPrevious re-wraps the prior comment as a "#prevNum" drop-down and carries
// its older drop-downs alongside as siblings. Returns "" when there is no prior.
func foldPrevious(prev string, prevNum int) string {
	if strings.TrimSpace(prev) == "" {
		return ""
	}
	var kept []string
	for line := range strings.SplitSeq(prev, "\n") {
		if line == marker || perfEvalRe.MatchString(line) || seriesHeadingRe.MatchString(line) {
			continue
		}
		kept = append(kept, line)
	}
	current, older := splitAtFirstDetails(kept)

	var b strings.Builder
	fmt.Fprintf(&b, "\n<details>\n<summary>Performance Evaluation Test #%d</summary>\n\n", prevNum)
	b.WriteString(strings.Trim(strings.Join(current, "\n"), "\n"))
	b.WriteString("\n</details>\n")
	if olderStr := strings.Trim(strings.Join(older, "\n"), "\n"); olderStr != "" {
		b.WriteString("\n" + olderStr + "\n")
	}
	return b.String()
}

// splitAtFirstDetails partitions lines at the first "<details>" line: the lines
// before it (the prior run's own results) and the rest (older runs' drop-downs).
func splitAtFirstDetails(lines []string) ([]string, []string) {
	for i, line := range lines {
		if line == "<details>" {
			return lines[:i], lines[i:]
		}
	}
	return lines, nil
}

// parsePrevNumber returns the test number in a prior comment's perf-eval marker,
// or 0 when there is no prior comment.
func parsePrevNumber(body string) int {
	for line := range strings.SplitSeq(body, "\n") {
		if m := perfEvalRe.FindStringSubmatch(line); m != nil {
			n, _ := strconv.Atoi(m[1])
			return n
		}
	}
	return 0
}

// fetchResult returns the leg result object's verdict and rendered markdown, or
// ""s when the object is missing or unreadable (a timeout or pre-publish failure).
func fetchResult(ctx context.Context, client *s3.Client, bucket, key string) (string, string) {
	if bucket == "" || key == "" {
		return "", ""
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		logger.Warnf("no result object at s3://%s/%s: %v", bucket, key, err)
		return "", ""
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		logger.Warnf("reading s3://%s/%s: %v", bucket, key, err)
		return "", ""
	}
	var res struct {
		Verdict  string `json:"verdict"`
		Markdown string `json:"markdown"`
	}
	if err := json.Unmarshal(data, &res); err != nil {
		logger.Warnf("decoding s3://%s/%s: %v", bucket, key, err)
		return "", ""
	}
	return res.Verdict, res.Markdown
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
