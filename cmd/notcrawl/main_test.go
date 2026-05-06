package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	if err := st.UpsertBlock(ctx, store.Block{
		ID:           "block1",
		PageID:       "page1",
		ParentID:     "page1",
		Type:         "bulleted_list",
		Text:         "sync launch checklist",
		DisplayOrder: 1,
		Alive:        true,
		Source:       "test",
		SyncedAt:     now,
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
	if len(rows) == 0 || rows[0]["title"] != "Launch Plan" || rows[0]["source"] != "notion" || rows[0]["kind"] != "page" || rows[0]["container"] != "Roadmap" || !strings.Contains(fmt.Sprint(rows[0]["text"]), "sync launch checklist") || !strings.Contains(fmt.Sprint(rows[0]["detail"]), "sync launch checklist") {
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

func TestTUIAllRowsIncludesDatabasesWhenPagesHitLimit(t *testing.T) {
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
	for _, title := range []string{"Launch Plan", "Backlog"} {
		if err := st.UpsertPage(ctx, store.Page{ID: title, CollectionID: "db1", Title: title, Alive: true, Source: "test", SyncedAt: now, LastEditedTime: now}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	err = run(ctx, []string{"--config", filepath.Join(dir, "missing.toml"), "--db", dbPath, "tui", "--json", "--limit", "1"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("tui --json failed: %v\nstderr:\n%s", err, stderr.String())
	}
	var rows []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatalf("invalid json: %v\n%s", err, stdout.String())
	}
	seen := map[string]bool{}
	for _, row := range rows {
		seen[fmt.Sprint(row["kind"])] = true
	}
	if !seen["page"] || !seen["database"] {
		t.Fatalf("all rows should include pages and databases despite page limit: %#v", rows)
	}
}

func TestCollectionTUIRowsResolveParentCollectionNames(t *testing.T) {
	rows := collectionTUIRows([]store.Collection{{
		ID:          "child-db",
		SpaceID:     "space1",
		ParentID:    "parent-db",
		ParentTable: "collection",
		Name:        "Child",
		Source:      "test",
	}}, 10, nil, map[string]string{"parent-db": "Parent Database"}, map[string]string{"space1": "Workspace"})
	if len(rows) != 1 {
		t.Fatalf("rows = %#v", rows)
	}
	if rows[0].ParentID != "Parent Database" {
		t.Fatalf("parent label = %q", rows[0].ParentID)
	}
	if rows[0].Scope != "Workspace" {
		t.Fatalf("scope = %q", rows[0].Scope)
	}
	if !strings.Contains(rows[0].Detail, "Parent: Parent Database") {
		t.Fatalf("detail = %q", rows[0].Detail)
	}
}

func TestTUIRowsHideRawNotionParentIDs(t *testing.T) {
	rows := pageTUIRows([]store.Page{{
		ID:             "page1",
		SpaceID:        "space1",
		ParentID:       "space:00b8cbcf-c520-4790-999a-9c2940263721",
		ParentTable:    "space",
		CollectionID:   "",
		Title:          "Launch Plan",
		Alive:          true,
		Source:         "test",
		LastEditedTime: 1000,
	}}, 10, nil, nil, map[string]string{"space1": "Comet.com", "00b8cbcf-c520-4790-999a-9c2940263721": "Comet.com"}, nil, nil)
	if len(rows) != 1 {
		t.Fatalf("rows = %#v", rows)
	}
	if rows[0].ParentID != "Workspace: Comet.com" {
		t.Fatalf("parent label = %q", rows[0].ParentID)
	}

	rows = pageTUIRows([]store.Page{{
		ID:          "page2",
		SpaceID:     "space1",
		ParentID:    "330b54b1-d7cc-4cd7-96bc-4d705b5f37bf",
		ParentTable: "block",
		Title:       "Nested",
		Alive:       true,
		Source:      "test",
	}}, 10, nil, nil, map[string]string{"space1": "Comet.com"}, nil, nil)
	if rows[0].ParentID != "Workspace: Comet.com" {
		t.Fatalf("workspace fallback parent = %q", rows[0].ParentID)
	}
}

func TestTUIRowsHideNoisyNotionBlockParentLabels(t *testing.T) {
	rows := pageTUIRows([]store.Page{{
		ID:          "page1",
		SpaceID:     "space1",
		ParentID:    "block1",
		ParentTable: "block",
		Title:       "Child",
		Alive:       true,
		Source:      "test",
	}}, 10, map[string]string{
		"block1": "ce 2fd71240-10a3-80a0-a65a-007aec07c0d9 00b8cbcf-c520-4790-999a-9c2940263721 Pods",
	}, nil, map[string]string{"space1": "Comet.com"}, nil, nil)
	if len(rows) != 1 {
		t.Fatalf("rows = %#v", rows)
	}
	if rows[0].ParentID != "Workspace: Comet.com" {
		t.Fatalf("noisy parent label = %q", rows[0].ParentID)
	}
}

func TestTUIRowsResolveBlockParentToOwningPage(t *testing.T) {
	rows := pageTUIRows([]store.Page{{
		ID:          "page1",
		SpaceID:     "space1",
		ParentID:    "block-child",
		ParentTable: "block",
		Title:       "Nested",
		Alive:       true,
		Source:      "test",
	}}, 10, map[string]string{
		"parent-page": "Customer Folder",
	}, nil, map[string]string{"space1": "Comet.com"}, map[string]store.ParentRef{
		"block-child":  {ID: "block-parent", Table: "block"},
		"block-parent": {ID: "parent-page", Table: "page"},
	}, nil)
	if len(rows) != 1 {
		t.Fatalf("rows = %#v", rows)
	}
	if rows[0].ParentID != "Customer Folder" {
		t.Fatalf("resolved parent label = %q", rows[0].ParentID)
	}
}

func TestBlockPreviewKeepsNotionPageShape(t *testing.T) {
	blocks := []store.Block{
		{Type: "heading_1", Text: "Launch Plan"},
		{Type: "bulleted_list", Text: "ship tui"},
		{Type: "to_do", Text: "verify local binary"},
		{Type: "numbered_list", Text: "open terminal"},
		{Type: "quote", Text: "keep it readable"},
		{Type: "code", Text: "notcrawl tui"},
	}
	got := blockPreview(blocks, tuiPagePreviewMax)
	for _, want := range []string{"# Launch Plan", "- ship tui", "- [ ] verify local binary", "1. open terminal", "> keep it readable", "    notcrawl tui"} {
		if !strings.Contains(got, want) {
			t.Fatalf("preview missing %q:\n%s", want, got)
		}
	}
}

func TestBlockPreviewCleansLegacyNotionMarkers(t *testing.T) {
	got := blockPreview([]store.Block{
		{Type: "paragraph", Text: "Option A: b"},
		{Type: "paragraph", Text: "Marketing Customer Reference Rights a https://example.com/sheet"},
	}, tuiPagePreviewMax)
	if strings.Contains(got, " a https://") || strings.Contains(got, ": b") {
		t.Fatalf("preview leaked legacy markers:\n%s", got)
	}
	for _, want := range []string{"Option A:", "Marketing Customer Reference Rights <https://example.com/sheet>"} {
		if !strings.Contains(got, want) {
			t.Fatalf("preview missing %q:\n%s", want, got)
		}
	}
}

func TestBlockPreviewCompactsRepeatedLinkedPages(t *testing.T) {
	got := blockPreview([]store.Block{{
		Type: "paragraph",
		Text: "linked page, linked page, linked page Add details",
	}}, tuiPagePreviewMax)
	if got != "linked pages Add details" {
		t.Fatalf("got %q", got)
	}
}

func TestPagePreviewIncludesComments(t *testing.T) {
	got := pagePreview(
		[]store.Block{{Type: "paragraph", Text: "status update"}},
		[]store.Comment{{Text: "looks good"}, {Text: "ship it"}},
		tuiPagePreviewMax,
	)
	for _, want := range []string{"status update", "## Comments", "- looks good", "- ship it"} {
		if !strings.Contains(got, want) {
			t.Fatalf("page preview missing %q:\n%s", want, got)
		}
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

func TestHelpAfterGlobalFlagsHasNoSideEffects(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"--config", configPath, "--db", filepath.Join(dir, "notcrawl.db"), "--help"}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Usage of notcrawl:") || !strings.Contains(stdout.String(), "tui") {
		t.Fatalf("help missing usage:\n%s", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("unexpected stderr:\n%s", stderr.String())
	}
	if _, err := os.Stat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("help should not write config, stat err=%v", err)
	}
}

func TestInitHelpDoesNotWriteConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"--config", configPath, "init", "--help"}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Usage of init:") {
		t.Fatalf("init help missing usage:\n%s", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("unexpected stderr:\n%s", stderr.String())
	}
	if _, err := os.Stat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("init --help should not write config, stat err=%v", err)
	}
}

func TestVersionFlagWorksWithOtherGlobalFlags(t *testing.T) {
	var stdout bytes.Buffer
	err := run(context.Background(), []string{"--config", filepath.Join(t.TempDir(), "missing.toml"), "--version"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(stdout.String()); got != version {
		t.Fatalf("version = %q", got)
	}
}

func TestMetadataDoesNotMarkPlainTextCommandsAsJSON(t *testing.T) {
	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"metadata"}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Commands map[string]struct {
			JSON bool `json:"json"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &manifest); err != nil {
		t.Fatalf("invalid metadata JSON: %v\n%s", err, stdout.String())
	}
	for _, name := range []string{"sync", "tap", "publish", "subscribe", "update"} {
		if manifest.Commands[name].JSON {
			t.Fatalf("%s should not be advertised as JSON", name)
		}
	}
	for _, name := range []string{"status", "doctor", "tui-json"} {
		if !manifest.Commands[name].JSON {
			t.Fatalf("%s should be advertised as JSON", name)
		}
	}
}

func TestSyncEmitsProgressPercentToStderr(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--config", filepath.Join(dir, "missing.toml"),
		"--db", filepath.Join(dir, "notcrawl.db"),
		"sync", "--source", "desktop",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("sync failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	logs := stderr.String()
	for _, want := range []string{`msg="sync progress"`, `state=finished`, `percent=100.0`, `completion=100.0%`, `phase=desktop`} {
		if !strings.Contains(logs, want) {
			t.Fatalf("missing %q in progress logs:\n%s", want, logs)
		}
	}
}

func TestTUIHelpReturnsUsage(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{"tui", "--help"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Usage of tui:") || !strings.Contains(stdout.String(), "-limit") || !strings.Contains(stdout.String(), "right-click") || !strings.Contains(stdout.String(), "#              jump") {
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
