package store

import (
	"context"
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
