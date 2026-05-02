package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vincentkoc/notcrawl/internal/store"
)

func TestSearchFieldCollapsesRecordSeparators(t *testing.T) {
	got := searchField("line one\nline\ttwo  line three")
	if got != "line one line two line three" {
		t.Fatalf("unexpected field: %q", got)
	}
}

func TestTUIJSONListsArchiveRowsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "notcrawl.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	now := store.NowMS()
	if err := st.UpsertCollection(ctx, store.Collection{ID: "db1", Name: "Roadmap", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPage(ctx, store.Page{
		ID:             "page1",
		CollectionID:   "db1",
		Title:          "Launch Plan",
		URL:            "https://example.com/launch",
		Alive:          true,
		Source:         "test",
		SyncedAt:       now,
		LastEditedTime: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	err = run(ctx, []string{"--config", filepath.Join(dir, "missing.toml"), "--db", dbPath, "tui", "--json"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("tui --json failed: %v\nstderr:\n%s", err, stderr.String())
	}
	var rows []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatalf("invalid json: %v\n%s", err, stdout.String())
	}
	if len(rows) == 0 || rows[0]["title"] != "Launch Plan" || rows[0]["source"] != "notion" || rows[0]["kind"] != "page" || rows[0]["container"] != "db1" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("tui --json mutated the sqlite database")
	}
}

func TestHelpMentionsTUI(t *testing.T) {
	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"--help"}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "tui") {
		t.Fatalf("help missing tui command:\n%s", stdout.String())
	}
}

func TestTUIHelpReturnsUsage(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{"tui", "--help"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Usage of tui:") || !strings.Contains(stdout.String(), "-limit") {
		t.Fatalf("tui help missing usage:\n%s", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("unexpected stderr:\n%s", stderr.String())
	}
}

func TestExportDatabaseAllWritesFilesAndIndex(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "notcrawl.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	now := store.NowMS()
	for _, collection := range []store.Collection{
		{ID: "db1", Name: "Roadmap", Source: "test", SyncedAt: now, SchemaJSON: `{"Name":{"type":"title"}}`},
		{ID: "db2", Name: "Launch 🚀 Plan ✅", Source: "test", SyncedAt: now, SchemaJSON: `{"Task":{"type":"title"}}`},
	} {
		if err := st.UpsertCollection(ctx, collection); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.UpsertPage(ctx, store.Page{
		ID: "page1", CollectionID: "db1", Title: "Ship", URL: "https://example.com/ship", Alive: true, Source: "test", SyncedAt: now,
		PropertiesJSON: `{"Name":{"type":"title","title":[{"plain_text":"Ship"}]}}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(dir, "csv")
	var stdout, stderr bytes.Buffer
	err = run(ctx, []string{"--config", filepath.Join(dir, "missing.toml"), "--db", dbPath, "export-db", "--all", "--dir", outDir}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("export-db --all failed: %v\nstderr:\n%s", err, stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, "exported 2 databases and 1 rows") {
		t.Fatalf("unexpected stdout: %s", got)
	}
	for _, name := range []string{"roadmap-db1.csv", "launch-plan-db2.csv", "index.tsv"} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	index, err := os.ReadFile(filepath.Join(outDir, "index.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"id\tname\tsource\trows\tcolumns\tfile", "db1\tRoadmap\ttest\t1\t4\troadmap-db1.csv"} {
		if !strings.Contains(string(index), want) {
			t.Fatalf("index missing %q:\n%s", want, index)
		}
	}
}
