package markdown

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vincentkoc/notioncrawl/internal/store"
)

func TestExporterWritesMarkdown(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notioncrawl.db"))
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
