package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreUpsertsAndSearchesPage(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	if err := st.UpsertPage(ctx, Page{ID: "page1", Title: "Launch Plan", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertBlock(ctx, Block{ID: "block1", PageID: "page1", Type: "text", Text: "ship the sqlite archive", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	results, err := st.Search(ctx, "sqlite", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one result, got %d", len(results))
	}
	if results[0].ID != "page1" {
		t.Fatalf("expected page1, got %q", results[0].ID)
	}
}

func TestStoreStatusAndOptimize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notcrawl.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	if err := st.UpsertPage(ctx, Page{ID: "page1", Title: "Launch Plan", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertBlock(ctx, Block{ID: "block1", PageID: "page1", Type: "text", Text: "maintenance keeps search sharp", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetSyncState(ctx, "test", "workspace", "default", "done"); err != nil {
		t.Fatal(err)
	}

	status, err := st.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pages != 1 || status.Blocks != 1 || status.LastSyncAt == 0 || status.DBBytes == 0 {
		t.Fatalf("unexpected status: %+v", status)
	}

	summary, err := st.Optimize(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.RebuiltFTS || !summary.Optimized || !summary.Analyzed || summary.Vacuumed {
		t.Fatalf("unexpected maintenance summary: %+v", summary)
	}
	results, err := st.Search(ctx, "maintenance", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "page1" {
		t.Fatalf("unexpected search results after optimize: %+v", results)
	}
}

func TestStoreOpenAppliesSQLitePragmasAndPrivateFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notcrawl.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("database should not be group/world-readable: %s", info.Mode().Perm())
	}

	var journalMode string
	if err := st.DB().QueryRow(`pragma journal_mode`).Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode != "wal" {
		t.Fatalf("expected WAL journal mode, got %q", journalMode)
	}
	var busyTimeout int
	if err := st.DB().QueryRow(`pragma busy_timeout`).Scan(&busyTimeout); err != nil {
		t.Fatal(err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("expected busy_timeout=5000, got %d", busyTimeout)
	}
}
