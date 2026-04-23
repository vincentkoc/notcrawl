package tableexport

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vincentkoc/notcrawl/internal/store"
)

func TestExportDatabaseTSV(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := store.NowMS()
	if err := st.UpsertCollection(ctx, store.Collection{
		ID: "db1", Name: "Roadmap", Source: "test", SyncedAt: now,
		SchemaJSON: `{"Name":{"type":"title"},"Status":{"type":"select"},"Score":{"type":"number"}}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPage(ctx, store.Page{
		ID: "page1", CollectionID: "db1", Title: "Ship", URL: "https://example.com/ship", Alive: true, Source: "test", SyncedAt: now,
		PropertiesJSON: `{"Name":{"type":"title","title":[{"plain_text":"Ship"}]},"Status":{"type":"select","select":{"name":"Done"}},"Score":{"type":"number","number":7}}`,
	}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	s, err := (Exporter{Store: st}).Export(ctx, "db1", FormatTSV, &out)
	if err != nil {
		t.Fatal(err)
	}
	if s.Rows != 1 {
		t.Fatalf("expected one row, got %d", s.Rows)
	}
	got := out.String()
	for _, want := range []string{"page_id\tpage_title\turl\tName\tScore\tStatus", "page1\tShip\thttps://example.com/ship\tShip\t7\tDone"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}
