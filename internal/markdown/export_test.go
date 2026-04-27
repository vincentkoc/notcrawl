package markdown

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vincentkoc/notcrawl/internal/store"
)

func TestExporterWritesMarkdown(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := store.NowMS()
	if err := st.UpsertSpace(ctx, store.Space{ID: "space1", Name: "Engineering", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPage(ctx, store.Page{ID: "page1", SpaceID: "space1", Title: "Launch Plan", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertBlock(ctx, store.Block{ID: "block1", PageID: "page1", ParentID: "page1", Type: "bulleted_list", Text: "ship it", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	s, err := Exporter{Store: st, Dir: dir}.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Pages != 1 || len(s.Files) != 1 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	b, err := os.ReadFile(s.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if !strings.Contains(text, "# Launch Plan") || !strings.Contains(text, "- ship it") {
		t.Fatalf("unexpected markdown:\n%s", text)
	}
}

func TestExporterUsesDisplayOrder(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := store.NowMS()
	if err := st.UpsertPage(ctx, store.Page{ID: "page1", Title: "Recipe", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	for _, block := range []store.Block{
		{ID: "salt", PageID: "page1", ParentID: "page1", Type: "bulleted_list", Text: "salt", DisplayOrder: 2, CreatedTime: now, Alive: true, Source: "test", SyncedAt: now},
		{ID: "flour", PageID: "page1", ParentID: "page1", Type: "bulleted_list", Text: "flour", DisplayOrder: 1, CreatedTime: now, Alive: true, Source: "test", SyncedAt: now},
	} {
		if err := st.UpsertBlock(ctx, block); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	s, err := Exporter{Store: st, Dir: dir}.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(s.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if strings.Index(text, "- flour") > strings.Index(text, "- salt") {
		t.Fatalf("markdown did not preserve display order:\n%s", text)
	}
}

func TestExporterPreservesUnicodePathNames(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := store.NowMS()
	if err := st.UpsertSpace(ctx, store.Space{ID: "space1", Name: "研究 🚀", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPage(ctx, store.Page{ID: "page1", SpaceID: "space1", Title: "計画 ✅ / Q2", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	s, err := Exporter{Store: st, Dir: dir}.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "研究-🚀", "計画-✅-q2-page1.md")
	if len(s.Files) != 1 || s.Files[0] != want {
		t.Fatalf("unexpected export path: %+v, want %s", s.Files, want)
	}
}

func TestExporterUsesWorkspaceAndTeamspacePath(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := store.NowMS()
	if err := st.UpsertSpace(ctx, store.Space{ID: "space1", Name: "Acme Org", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertTeam(ctx, store.Team{ID: "team1", SpaceID: "space1", Name: "Research Lab", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPage(ctx, store.Page{ID: "page1", SpaceID: "space1", ParentID: "team1", ParentTable: "team", Title: "Plan", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	s, err := Exporter{Store: st, Dir: dir}.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "acme-org", "research-lab", "plan-page1.md")
	if len(s.Files) != 1 || s.Files[0] != want {
		t.Fatalf("unexpected export path: %+v, want %s", s.Files, want)
	}
	b, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	if !strings.Contains(text, `team_id: "team1"`) || !strings.Contains(text, `team: "Research Lab"`) {
		t.Fatalf("missing team front matter:\n%s", text)
	}
}

func TestExporterUsesReadableMissingSpaceFallback(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := store.NowMS()
	spaceID := "52f1c029-ec85-4ff5-bd43-c6d6ea9259e0"
	if err := st.UpsertPage(ctx, store.Page{ID: "page1", SpaceID: spaceID, Title: "Loose", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	s, err := Exporter{Store: st, Dir: dir}.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "space-52f1c029-ea9259e0", "loose-page1.md")
	if len(s.Files) != 1 || s.Files[0] != want {
		t.Fatalf("unexpected export path: %+v, want %s", s.Files, want)
	}
}

func TestExporterPrunesStaleMarkdown(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := store.NowMS()
	if err := st.UpsertPage(ctx, store.Page{ID: "page1", Title: "Launch", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	staleDir := filepath.Join(dir, "old")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	staleMarkdown := filepath.Join(staleDir, "stale.md")
	if err := os.WriteFile(staleMarkdown, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	keepNote := filepath.Join(staleDir, "note.txt")
	if err := os.WriteFile(keepNote, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := (Exporter{Store: st, Dir: dir}).Export(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staleMarkdown); !os.IsNotExist(err) {
		t.Fatalf("expected stale markdown to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(keepNote); err != nil {
		t.Fatalf("expected non-markdown file to remain: %v", err)
	}
}
