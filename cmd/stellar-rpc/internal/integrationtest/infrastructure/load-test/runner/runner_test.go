package main

import "testing"

func TestClassifyPollOutput(t *testing.T) {
	cases := []struct {
		name, out     string
		state         pollState
		verdict, body string
	}{
		{"empty is not ready", "", pollNotReady, "", ""},
		{"sentinel not ready", "__NOT_READY__\n", pollNotReady, "", ""},
		{"download complete", "__DOWNLOAD_COMPLETE__\n", pollDownloadComplete, "", ""},
		{"ok with body", "ok\n# Results\n| a | b |", pollDone, "ok", "# Results\n| a | b |"},
		{"fail verdict, empty body", "fail\n", pollDone, "fail", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state, verdict, body := classifyPollOutput(c.out)
			if state != c.state || verdict != c.verdict || body != c.body {
				t.Fatalf("got (%d, %q, %q), want (%d, %q, %q)",
					state, verdict, body, c.state, c.verdict, c.body)
			}
		})
	}
}
