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
		SchemaJSON: `{"title":{"name":"Name","type":"title"},"assignee_id":{"name":"Assignee","type":"person"},"due_id":{"name":"Due","type":"date"},"status_id":{"name":"Status","type":"select"},"score_id":{"name":"Score","type":"number"}}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertUser(ctx, store.User{ID: "user1", Name: "Claire Pena", Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPage(ctx, store.Page{
		ID: "page1", CollectionID: "db1", Title: "Ship", URL: "https://example.com/ship", Alive: true, Source: "test", SyncedAt: now,
		PropertiesJSON: `{"title":{"type":"title","title":[{"plain_text":"Ship"}]},"status_id":{"type":"select","select":{"name":"Done"}},"score_id":{"type":"number","number":7}}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertPage(ctx, store.Page{
		ID: "page2", ParentID: "db1", ParentTable: "collection", Title: "Draft", URL: "https://example.com/draft", Alive: true, Source: "test", SyncedAt: now,
		PropertiesJSON: `{"title":[["Draft"]],"assignee_id":[["‣",[["u","user1"]]]],"due_id":[["‣",[["d",{"type":"date","start_date":"2025-05-23"}]]]],"status_id":[["In progress"]],"score_id":[["3"]]}`,
	}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	s, err := (Exporter{Store: st}).Export(ctx, "db1", FormatTSV, &out)
	if err != nil {
		t.Fatal(err)
	}
	if s.Rows != 2 {
		t.Fatalf("expected two rows, got %d", s.Rows)
	}
	got := out.String()
	for _, want := range []string{
		"page_id\tpage_title\turl\tName\tAssignee\tDue\tScore\tStatus",
		"page1\tShip\thttps://example.com/ship\tShip\t\t\t7\tDone",
		"page2\tDraft\thttps://example.com/draft\tDraft\tClaire Pena\t2025-05-23\t3\tIn progress",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}
