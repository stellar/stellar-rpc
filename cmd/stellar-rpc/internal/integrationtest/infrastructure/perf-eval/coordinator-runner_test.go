package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func okLeg(md string) []legResult {
	return []legResult{{Label: "Apply-load ingestion", Verdict: "ok", Markdown: md}}
}

func renderN(n int, legs []legResult) string {
	var prev string
	for i := range n {
		prev = renderComment(runRecord{
			TargetSHA: strings.Repeat("a", 12),
			TargetRef: "release/v1",
			RunURL:    fmt.Sprintf("https://example/run/%d", i+1),
			Legs:      legs,
		}, prev)
	}
	return prev
}

func TestRenderComment_FirstRun(t *testing.T) {
	out := renderComment(runRecord{
		TargetSHA: "abcdef1234567890",
		TargetRef: "release/v1.2.3",
		RunURL:    "https://example/run/1",
		Legs:      okLeg("throughput: 42 l/s"),
	}, "")
	require.True(t, strings.HasPrefix(out, marker+"\n"))
	require.Contains(t, out, "## 🧪 Performance Evaluation Test #1")
	require.Contains(t, out, "**Commit:** `abcdef123456` (`release/v1.2.3`)")
	require.Contains(t, out, "### ✅ Apply-load ingestion — verdict: ok")
	require.Contains(t, out, "throughput: 42 l/s")
	require.NotContains(t, out, "<details>") // no history on the first run
}

func TestRenderLeg_FallbackWhenNoResult(t *testing.T) {
	out := renderLeg(legResult{Label: "X"})
	require.Contains(t, out, "### ❌ X — verdict: none")
	require.Contains(t, out, "No result object published")
}

// Four runs in sequence: each run's output is the next run's "prior comment".
// The result must keep the current run on top and the rest as drop-downs.
func TestRenderComment_FoldsHistory(t *testing.T) {
	out := renderN(4, okLeg("run body"))

	require.Equal(t, 1, strings.Count(out, marker))
	require.Contains(t, out, "## 🧪 Performance Evaluation Test #4")
	require.Equal(t, 3, strings.Count(out, "<details>\n<summary>Performance Evaluation Test #"))

	// Each prior run in descending order (#3, then #2, then #1)
	i3 := strings.Index(out, "Performance Evaluation Test #3")
	i2 := strings.Index(out, "Performance Evaluation Test #2")
	i1 := strings.Index(out, "Performance Evaluation Test #1")
	require.Positive(t, i3)
	require.Less(t, i3, i2)
	require.Less(t, i2, i1)
}

// Leg markdown containing its own <details> block must survive folding intact:
// the history is data, so rendered markup is never parsed back.
func TestRenderComment_LegMarkdownWithDetailsSurvivesFold(t *testing.T) {
	legMD := "table\n\n<details>\n<summary>per-profile breakdown</summary>\n\ninner\n</details>"
	out := renderN(3, okLeg(legMD))

	hist := parseHistory(out)
	require.Len(t, hist, 3)
	for _, r := range hist {
		require.Equal(t, legMD, r.Legs[0].Markdown)
	}
	// The inner drop-down renders in every fold without hijacking the structure.
	require.Equal(t, 3, strings.Count(out, "<summary>per-profile breakdown</summary>"))
	require.Equal(t, 2, strings.Count(out, "<summary>Performance Evaluation Test #"))
}

// Numbering continues past the history cap; the oldest runs are shed.
func TestRenderComment_CapsHistory(t *testing.T) {
	out := renderN(maxHistory+2, okLeg("run body"))

	hist := parseHistory(out)
	require.Len(t, hist, maxHistory)
	require.Equal(t, maxHistory+2, hist[0].Num)
	require.Contains(t, out, fmt.Sprintf("## 🧪 Performance Evaluation Test #%d", maxHistory+2))
	require.NotContains(t, out, "Performance Evaluation Test #2\n") // shed
	require.NotContains(t, out, "Performance Evaluation Test #1\n") // shed
}

// Oversized histories shed old runs to stay under the comment-size cap.
func TestRenderComment_ShedsRunsOverSizeCap(t *testing.T) {
	big := strings.Repeat("x", maxCommentLen/3)
	out := renderN(5, okLeg(big))

	require.LessOrEqual(t, len(out), maxCommentLen)
	hist := parseHistory(out)
	require.NotEmpty(t, hist)
	require.Equal(t, 5, hist[0].Num) // current run always kept
}

// A prior comment without a parseable blob (legacy, corrupt, or hand-edited)
// starts the series fresh instead of failing.
func TestParseHistory_FreshOnAbsentOrCorrupt(t *testing.T) {
	require.Nil(t, parseHistory(""))
	require.Nil(t, parseHistory(marker+"\n## some legacy comment\n"))
	require.Nil(t, parseHistory("<!-- perf-eval-history: !!!not-base64!!! -->"))
	require.Nil(t, parseHistory("<!-- perf-eval-history: bm90LWpzb24= -->")) // "not-json"

	out := renderComment(runRecord{
		TargetSHA: strings.Repeat("a", 12),
		TargetRef: "release/v1",
		RunURL:    "https://example/run",
		Legs:      okLeg("body"),
	}, marker+"\n## some legacy comment\n")
	require.Contains(t, out, "## 🧪 Performance Evaluation Test #1")
}
