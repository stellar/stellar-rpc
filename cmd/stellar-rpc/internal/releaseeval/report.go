package releaseeval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// areaStatus is what a placeholder (or future real) child job writes to
// artifacts/area-<name>/status.json.
type areaStatus struct {
	Area    string             `json:"area"`
	Status  string             `json:"status"`
	Reason  string             `json:"reason"`
	Metrics map[string]float64 `json:"metrics"`
}

// row is one line of the rendered report (and of the report.json areas array).
type row struct {
	Area    string             `json:"area"`
	Status  string             `json:"status"`
	Reason  string             `json:"reason"`
	Metrics map[string]float64 `json:"metrics,omitempty"`
}

type reportMeta struct {
	Ref      string
	Profile  string
	Baseline string
	Mode     string
	Overall  string
	RunURL   string
}

// RunGate aggregates the child-job results, writes the report (step summary +
// report.{md,json}), posts a PR comment when one exists, and returns true if the
// release gate failed. It is the entry point invoked by TestReleaseGate.
func RunGate() bool {
	artifactsDir := getenv("ARTIFACTS_DIR", "artifacts")
	aggDir := getenv("AGG_DIR", "aggregated")

	cfg, err := parseThresholds()
	if err != nil {
		fmt.Fprintf(os.Stdout, "::warning::invalid embedded thresholds.json: %v\n", err)
	}
	mode := resolveMode(os.Getenv("THRESHOLD_MODE"), cfg.Mode)

	// Placeholder (and future file-based) areas carry their own status; mode only
	// decides whether a `fail` actually gates.
	areaRows := readAreaStatuses(artifactsDir)
	rows := make([]row, 0, len(areaRows)+1)
	gateFail := false
	for _, areaRow := range areaRows {
		rows = append(rows, areaRow)
		if areaRow.Status == statusFail && mode == modeEnforce {
			gateFail = true
		}
	}

	// Core apply-load: the EC2 verdict gates regardless of mode; metric
	// comparison layers on when the verdict passed.
	alRow, alGate := evalApplyLoad(applyLoadInput{
		result: os.Getenv("APPLY_RESULT"),
		passed: os.Getenv("APPLY_PASSED"),
		found:  os.Getenv("APPLY_FOUND"),
		perf:   readPerf(artifactsDir),
	}, cfg.Areas[applyLoadArea], mode)
	rows = append(rows, alRow)
	gateFail = gateFail || alGate

	meta := reportMeta{
		Ref:      os.Getenv("GITHUB_REF_NAME"),
		Profile:  getenv("PROFILE", "standard"),
		Baseline: getenv("BASELINE_REF", "main"),
		Mode:     mode,
		Overall:  statusPass,
		RunURL:   runURL(),
	}
	if gateFail {
		meta.Overall = statusFail
	}

	if err := os.MkdirAll(aggDir, 0o750); err != nil {
		fmt.Fprintf(os.Stdout, "::error::cannot create %s: %v\n", aggDir, err)
		return true
	}

	md := renderMarkdown(meta, rows)
	if embed := embedApplyLoad(artifactsDir); embed != "" {
		md += "\n" + embed + "\n"
	}
	reportMD := filepath.Join(aggDir, "report.md")
	if err := os.WriteFile(reportMD, []byte(md), 0o600); err != nil {
		fmt.Fprintf(os.Stdout, "::warning::failed to write %s: %v\n", reportMD, err)
	}
	if err := writeReportJSON(filepath.Join(aggDir, "report.json"), meta, rows); err != nil {
		fmt.Fprintf(os.Stdout, "::warning::failed to write report.json: %v\n", err)
	}
	appendSummary(md)
	maybeComment(os.Getenv("PR_NUMBER"), os.Getenv("GITHUB_REPOSITORY"), reportMD)

	if gateFail {
		fmt.Fprintf(os.Stdout, "::error::release gate FAILED (mode=%s)\n", mode)
	} else {
		fmt.Fprintf(os.Stdout, "release gate passed (mode=%s)\n", mode)
	}
	return gateFail
}

// readAreaStatuses reads every artifacts/area-*/status.json, sorted by path.
func readAreaStatuses(dir string) []row {
	matches, _ := filepath.Glob(filepath.Join(dir, "area-*", "status.json"))
	sort.Strings(matches)
	rows := make([]row, 0, len(matches))
	for _, f := range matches {
		b, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintf(os.Stdout, "::warning::cannot read %s: %v\n", f, err)
			continue
		}
		var st areaStatus
		if err := json.Unmarshal(b, &st); err != nil {
			fmt.Fprintf(os.Stdout, "::warning::invalid status.json %s: %v\n", f, err)
			continue
		}
		if st.Status == "" {
			st.Status = statusNeutral
		}
		rows = append(rows, row(st))
	}
	return rows
}

// readPerf loads the apply-load perf metrics, or nil when absent/invalid.
func readPerf(dir string) *PerfReport {
	b, err := os.ReadFile(filepath.Join(dir, "apply-load-results", "bench-results.json"))
	if err != nil {
		return nil
	}
	var p PerfReport
	if err := json.Unmarshal(b, &p); err != nil {
		fmt.Fprintf(os.Stdout, "::warning::invalid bench-results.json: %v\n", err)
		return nil
	}
	return &p
}

// embedApplyLoad returns the EC2 child's results table (or timeout diagnostics)
// wrapped in a collapsible block, or "" when absent.
func embedApplyLoad(dir string) string {
	for _, name := range []string{"results.md", "timeout-comment.md"} {
		b, err := os.ReadFile(filepath.Join(dir, "apply-load-results", name))
		if err == nil && len(b) > 0 {
			return "<details><summary>Core apply-load — full results</summary>\n\n" + string(b) + "\n</details>"
		}
	}
	return ""
}

func renderMarkdown(m reportMeta, rows []row) string {
	var b strings.Builder
	ref := m.Ref
	if ref == "" {
		ref = "local"
	}
	fmt.Fprintf(&b, "## Release Evaluation — `%s`\n\n", ref)
	fmt.Fprintf(&b, "Profile: **%s** · Baseline: `%s` · Threshold mode: **%s** · Overall: **%s**\n\n",
		m.Profile, m.Baseline, m.Mode, m.Overall)
	b.WriteString("| Area | Status | Notes |\n| --- | --- | --- |\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %s | %s | %s |\n", r.Area, r.Status, r.Reason)
	}
	b.WriteString("\n_Neutral areas are scaffolded placeholders or unconfigured (TBD) thresholds; " +
		"they do not fail the gate._\n")
	if m.RunURL != "" {
		fmt.Fprintf(&b, "\n[Workflow run](%s)\n", m.RunURL)
	}
	return b.String()
}

func writeReportJSON(path string, m reportMeta, rows []row) error {
	out := struct {
		Ref      string `json:"ref"`
		Profile  string `json:"profile"`
		Baseline string `json:"baseline"`
		Mode     string `json:"mode"`
		Overall  string `json:"overall"`
		RunURL   string `json:"runUrl"`
		Areas    []row  `json:"areas"`
	}{
		Ref:      m.Ref,
		Profile:  m.Profile,
		Baseline: m.Baseline,
		Mode:     m.Mode,
		Overall:  m.Overall,
		RunURL:   m.RunURL,
		Areas:    rows,
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func appendSummary(s string) {
	path := os.Getenv("GITHUB_STEP_SUMMARY")
	if path == "" {
		return
	}
	// path is GITHUB_STEP_SUMMARY, written by the Actions runner.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600) //nolint:gosec
	if err != nil {
		fmt.Fprintf(os.Stdout, "::warning::cannot append to step summary: %v\n", err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString(s + "\n"); err != nil {
		fmt.Fprintf(os.Stdout, "::warning::cannot write step summary: %v\n", err)
	}
}

// maybeComment posts the report to the PR when PR_NUMBER is set and gh exists.
func maybeComment(prNumber, repo, bodyFile string) {
	if prNumber == "" {
		return
	}
	if _, err := exec.LookPath("gh"); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// gh args are workflow-controlled: a numeric PR id, the repo slug, a path we wrote.
	cmd := exec.CommandContext( //nolint:gosec
		ctx, "gh", "pr", "comment", prNumber,
		"--repo", repo, "--body-file", bodyFile,
	)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stdout, "::warning::failed to comment on PR #%s: %v\n", prNumber, err)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runURL() string {
	server, repo, id := os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID")
	if server != "" && repo != "" && id != "" {
		return fmt.Sprintf("%s/%s/actions/runs/%s", server, repo, id)
	}
	return ""
}
