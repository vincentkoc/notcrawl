package notionapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/vincentkoc/notcrawl/internal/store"
)

func TestSyncIngestsDatabasesAndRows(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/users":
			_, _ = w.Write([]byte(`{"object":"list","results":[],"has_more":false}`))
		case "/search":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			filter := body["filter"].(map[string]any)
			switch filter["value"] {
			case "page":
				_, _ = w.Write([]byte(`{"object":"list","results":[],"has_more":false}`))
			case "database":
				_, _ = w.Write([]byte(`{
					"object":"list",
					"results":[{
						"object":"database",
						"id":"db1",
						"title":[{"plain_text":"Roadmap"}],
						"parent":{"type":"workspace","workspace":true},
						"properties":{
							"Name":{"id":"title","type":"title","title":{}},
							"Status":{"id":"status","type":"select","select":{}}
						}
					}],
					"has_more":false
				}`))
			default:
				t.Fatalf("unexpected search filter: %v", filter["value"])
			}
		case "/databases/db1/query":
			_, _ = w.Write([]byte(`{
				"object":"list",
				"results":[{
					"object":"page",
					"id":"page1",
					"created_time":"2026-01-01T00:00:00Z",
					"last_edited_time":"2026-01-02T00:00:00Z",
					"archived":false,
					"in_trash":false,
					"url":"https://notion.so/page1",
					"parent":{"type":"database_id","database_id":"db1"},
					"properties":{
						"Name":{"id":"title","type":"title","title":[{"plain_text":"Ship"}]},
						"Status":{"id":"status","type":"select","select":{"name":"Done"}}
					}
				}],
				"has_more":false
			}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	summary, err := (Client{BaseURL: server.URL, Version: "2022-06-28", Token: "secret"}).Sync(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Databases != 1 || summary.DatabaseRows != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	collections, err := st.Collections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(collections) != 1 || collections[0].ID != "db1" || collections[0].Name != "Roadmap" {
		t.Fatalf("unexpected collections: %+v", collections)
	}
	rows, err := st.CollectionPages(context.Background(), "db1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "page1" || rows[0].CollectionID != "db1" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}
