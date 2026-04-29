package main

import (
	"bytes"
	"context"
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
		{ID: "db2", Name: "Launch Plan", Source: "test", SyncedAt: now, SchemaJSON: `{"Task":{"type":"title"}}`},
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
