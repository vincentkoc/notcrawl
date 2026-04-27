package store

import (
	"context"
	"errors"
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

func TestStoreSearchRanksByRelevanceThenRecency(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	pages := []Page{
		{ID: "old", Title: "Old", LastEditedTime: now - 1000, Alive: true, Source: "test", SyncedAt: now},
		{ID: "new", Title: "New", LastEditedTime: now, Alive: true, Source: "test", SyncedAt: now},
	}
	for _, page := range pages {
		if err := st.UpsertPage(ctx, page); err != nil {
			t.Fatal(err)
		}
		if err := st.UpsertBlock(ctx, Block{ID: page.ID + "-block", PageID: page.ID, Type: "text", Text: "needle", Alive: true, Source: "test", SyncedAt: now}); err != nil {
			t.Fatal(err)
		}
	}

	results, err := st.Search(ctx, "needle", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) < 2 || results[0].ID != "new" || results[1].ID != "old" {
		t.Fatalf("expected newer equal-rank page first, got %+v", results)
	}
}

func TestStoreSearchIncludesComments(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	if err := st.UpsertPage(ctx, Page{ID: "page1", Title: "Launch", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertComment(ctx, Comment{ID: "comment1", PageID: "page1", Text: "needle from a comment", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}

	results, err := st.Search(ctx, "needle", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Kind != "comment" || results[0].ID != "comment1" || results[0].Title != "Launch" {
		t.Fatalf("expected comment search result with page title, got %+v", results)
	}
}

func TestStoreDefersPageFTSRefresh(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	err = st.DeferPageFTS(ctx, func() error {
		if err := st.UpsertPage(ctx, Page{ID: "page1", Title: "Launch Plan", Alive: true, Source: "test", SyncedAt: now}); err != nil {
			return err
		}
		if err := st.UpsertBlock(ctx, Block{ID: "block1", PageID: "page1", Type: "text", Text: "deferred sqlite refresh", Alive: true, Source: "test", SyncedAt: now}); err != nil {
			return err
		}
		results, err := st.Search(ctx, "sqlite", 10)
		if err != nil {
			return err
		}
		if len(results) != 0 {
			t.Fatalf("expected deferred FTS to stay stale inside callback, got %+v", results)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	results, err := st.Search(ctx, "sqlite", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "page1" {
		t.Fatalf("expected refreshed FTS after callback, got %+v", results)
	}
}

func TestStoreTransactionCommitsAndRollsBack(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	if err := st.WithTransaction(ctx, func() error {
		return st.UpsertPage(ctx, Page{ID: "commit", Title: "Commit", Alive: true, Source: "test", SyncedAt: now})
	}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pages where id = 'commit'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected committed page, got %d", count)
	}

	sentinel := errors.New("rollback")
	err = st.WithTransaction(ctx, func() error {
		if err := st.UpsertPage(ctx, Page{ID: "rollback", Title: "Rollback", Alive: true, Source: "test", SyncedAt: now}); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected rollback error, got %v", err)
	}
	if err := st.DB().QueryRowContext(ctx, `select count(*) from pages where id = 'rollback'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected rolled back page, got %d", count)
	}
}

func TestStoreEnsuresFallbackSpaces(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	spaceID := "52f1c029-1111-2222-3333-ea9259e0"
	if err := st.UpsertPage(ctx, Page{ID: "page1", SpaceID: spaceID, Title: "Loose", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}

	added, err := st.EnsureSpaceFallbacks(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Fatalf("expected one fallback space, got %d", added)
	}
	name, err := st.SpaceName(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if name != "External Space 52f1c029-ea9259e0" {
		t.Fatalf("unexpected fallback space name: %q", name)
	}
	added, err = st.EnsureSpaceFallbacks(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if added != 0 {
		t.Fatalf("expected fallback insertion to be idempotent, got %d", added)
	}
}

func TestStoreOrdersBlocksByDisplayOrder(t *testing.T) {
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
	blocks := []Block{
		{ID: "third", PageID: "page1", ParentID: "page1", Type: "text", Text: "third", DisplayOrder: 3, CreatedTime: now, Alive: true, Source: "test", SyncedAt: now},
		{ID: "first", PageID: "page1", ParentID: "page1", Type: "text", Text: "first", DisplayOrder: 1, CreatedTime: now, Alive: true, Source: "test", SyncedAt: now},
		{ID: "second", PageID: "page1", ParentID: "page1", Type: "text", Text: "second", DisplayOrder: 2, CreatedTime: now, Alive: true, Source: "test", SyncedAt: now},
	}
	for _, block := range blocks {
		if err := st.UpsertBlock(ctx, block); err != nil {
			t.Fatal(err)
		}
	}
	got, err := st.PageBlocks(ctx, "page1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != "first" || got[1].ID != "second" || got[2].ID != "third" {
		t.Fatalf("unexpected block order: %+v", got)
	}
}

func TestStoreBuildsPageFTSInDisplayTreeOrder(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	if err := st.UpsertPage(ctx, Page{ID: "page1", Title: "Recipe", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	blocks := []Block{
		{ID: "z-root", PageID: "page1", ParentID: "page1", Type: "text", Text: "third", DisplayOrder: 2, CreatedTime: now, Alive: true, Source: "test", SyncedAt: now},
		{ID: "a-child", PageID: "page1", ParentID: "a-root", Type: "text", Text: "second", DisplayOrder: 1, CreatedTime: now, Alive: true, Source: "test", SyncedAt: now},
		{ID: "a-root", PageID: "page1", ParentID: "page1", Type: "text", Text: "first", DisplayOrder: 1, CreatedTime: now, Alive: true, Source: "test", SyncedAt: now},
	}
	for _, block := range blocks {
		if err := st.UpsertBlock(ctx, block); err != nil {
			t.Fatal(err)
		}
	}

	var body string
	if err := st.DB().QueryRowContext(ctx, `select body from page_fts where page_id = ?`, "page1").Scan(&body); err != nil {
		t.Fatal(err)
	}
	if body != "first\nsecond\nthird" {
		t.Fatalf("unexpected FTS body order: %q", body)
	}
}

func TestStoreResolvesPageTeamThroughCollectionParent(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	if err := st.UpsertTeam(ctx, Team{ID: "team1", SpaceID: "space1", Name: "Research", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertCollection(ctx, Collection{ID: "collection1", SpaceID: "space1", ParentID: "team1", ParentTable: "team", Name: "Roadmap", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	page := Page{ID: "page1", SpaceID: "space1", ParentID: "collection1", ParentTable: "collection", CollectionID: "collection1", Title: "Row", Alive: true, Source: "test", SyncedAt: now}
	if err := st.UpsertPage(ctx, page); err != nil {
		t.Fatal(err)
	}

	teamID, err := st.PageTeamID(ctx, page)
	if err != nil {
		t.Fatal(err)
	}
	if teamID != "team1" {
		t.Fatalf("expected team1, got %q", teamID)
	}
}

func TestStoreResolvesPageTeamThroughBlockParent(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := NowMS()
	if err := st.UpsertTeam(ctx, Team{ID: "team1", SpaceID: "space1", Name: "Research", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertBlock(ctx, Block{ID: "block1", SpaceID: "space1", ParentID: "team1", ParentTable: "team", Type: "text", Text: "parent", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	page := Page{ID: "page1", SpaceID: "space1", ParentID: "block1", ParentTable: "block", Title: "Child", Alive: true, Source: "test", SyncedAt: now}
	if err := st.UpsertPage(ctx, page); err != nil {
		t.Fatal(err)
	}

	teamID, err := st.PageTeamID(ctx, page)
	if err != nil {
		t.Fatal(err)
	}
	if teamID != "team1" {
		t.Fatalf("expected team1, got %q", teamID)
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
