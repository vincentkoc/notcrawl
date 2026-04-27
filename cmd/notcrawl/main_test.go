package main

import "testing"

func TestSearchFieldCollapsesRecordSeparators(t *testing.T) {
	got := searchField("line one\nline\ttwo  line three")
	if got != "line one line two line three" {
		t.Fatalf("unexpected field: %q", got)
	}
}
