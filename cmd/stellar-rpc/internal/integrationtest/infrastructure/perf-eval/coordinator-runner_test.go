package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func okLeg(md string) []leg {
	return []leg{{Label: "Apply-load ingestion", Passed: "true", Verdict: "ok", markdown: md}}
}

func TestParsePrevNumber(t *testing.T) {
	require.Equal(t, 0, parsePrevNumber(""))
	require.Equal(t, 0, parsePrevNumber("## no marker here\nbody"))
	require.Equal(t, 7, parsePrevNumber(marker+"\n<!-- perf-eval: 7 -->\n## 🧪 Performance Evaluation Test #7\n"))
}

func TestRenderComment_FirstRun(t *testing.T) {
	out := renderComment(commentInput{
		targetSHA: "abcdef1234567890",
		targetRef: "release/v1.2.3",
		runURL:    "https://example/run/1",
		legs:      okLeg("throughput: 42 l/s"),
	})
	require.True(t, strings.HasPrefix(out, marker+"\n"))
	require.Contains(t, out, "<!-- perf-eval: 1 -->")
	require.Contains(t, out, "## 🧪 Performance Evaluation Test #1")
	require.Contains(t, out, "**Commit:** `abcdef123456` (`release/v1.2.3`)")
	require.Contains(t, out, "### ✅ Apply-load ingestion — verdict: ok")
	require.Contains(t, out, "throughput: 42 l/s")
	require.NotContains(t, out, "<details>") // no history on the first run
}

func TestRenderLeg_FallbackWhenNoResult(t *testing.T) {
	out := renderLeg(leg{Label: "X", Passed: "false", Verdict: ""})
	require.Contains(t, out, "### ❌ X — verdict: none")
	require.Contains(t, out, "No result object published")
}

// Four runs in sequence: each run's output is the next run's "prior comment".
// The result must keep the current run on top and the rest as flat siblings.
func TestRenderComment_FoldsFlatHistory(t *testing.T) {
	var prev string
	for range 4 {
		prev = renderComment(commentInput{
			targetSHA: strings.Repeat("a", 12),
			targetRef: "release/v1",
			runURL:    "https://example/run",
			legs:      okLeg("run body"),
			prev:      prev,
		})
	}

	// Exactly one current run at the top
	require.Equal(t, 1, strings.Count(prev, marker))
	require.Equal(t, 1, countMatching(prev, perfEvalRe.MatchString))
	require.Equal(t, 1, countMatching(prev, seriesHeadingRe.MatchString))
	require.Contains(t, prev, "## 🧪 Performance Evaluation Test #4")

	// Three prior runs, each its own drop-down
	require.Equal(t, 3, strings.Count(prev, "<details>"))
	require.Equal(t, 3, strings.Count(prev, "</details>"))

	// Each run in descending order (#3, then #2, then #1)
	i3 := strings.Index(prev, "Performance Evaluation Test #3")
	i2 := strings.Index(prev, "Performance Evaluation Test #2")
	i1 := strings.Index(prev, "Performance Evaluation Test #1")
	require.Positive(t, i3)
	require.Less(t, i3, i2)
	require.Less(t, i2, i1)
}

func countMatching(s string, pred func(string) bool) int {
	n := 0
	for line := range strings.SplitSeq(s, "\n") {
		if pred(line) {
			n++
		}
	}
	return n
}
