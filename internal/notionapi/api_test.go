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
						"title":[{"type":"text","plain_text":"Roadmap","text":{"content":"Roadmap"}}],
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
						"Name":{"id":"title","type":"title","title":[{"type":"text","plain_text":"Ship","text":{"content":"Ship"}}]},
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
	if len(rows) != 1 || rows[0].ID != "page1" || rows[0].CollectionID != "db1" || rows[0].Title != "Ship" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestSyncIngestsCurrentDataSourcesAndRows(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/users":
			_, _ = w.Write([]byte(`{"object":"list","results":[],"has_more":false}`))
		case "/search":
			if got := r.Header.Get("Notion-Version"); got != "2026-03-11" {
				t.Fatalf("unexpected Notion-Version: %s", got)
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			filter := body["filter"].(map[string]any)
			switch filter["value"] {
			case "page":
				_, _ = w.Write([]byte(`{"object":"list","results":[],"has_more":false}`))
			case "data_source":
				_, _ = w.Write([]byte(`{
					"object":"list",
					"results":[{
						"object":"data_source",
						"id":"ds1",
						"title":[{"type":"text","plain_text":"Roadmap","text":{"content":"Roadmap"}}],
						"parent":{"type":"database_id","database_id":"db1"},
						"database_parent":{"type":"page_id","page_id":"page-parent"},
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
		case "/data_sources/ds1/query":
			_, _ = w.Write([]byte(`{
				"object":"list",
				"results":[{
					"object":"page",
					"id":"page1",
					"created_time":"2026-01-01T00:00:00Z",
					"last_edited_time":"2026-01-02T00:00:00Z",
					"in_trash":false,
					"url":"https://notion.so/page1",
					"parent":{"type":"data_source_id","data_source_id":"ds1"},
					"properties":{
						"Name":{"id":"title","type":"title","title":[{"type":"text","plain_text":"Ship","text":{"content":"Ship"}}]},
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

	summary, err := (Client{BaseURL: server.URL, Version: "2026-03-11", Token: "secret"}).Sync(context.Background(), st)
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
	if len(collections) != 1 || collections[0].ID != "ds1" || collections[0].ParentID != "db1" || collections[0].Name != "Roadmap" {
		t.Fatalf("unexpected collections: %+v", collections)
	}
	rows, err := st.CollectionPages(context.Background(), "ds1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "page1" || rows[0].CollectionID != "ds1" || rows[0].Title != "Ship" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestIngestCommentsSkipsRestrictedResource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/comments" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"object":"error","status":403,"code":"restricted_resource","message":"Insufficient permissions for this endpoint."}`))
	}))
	defer server.Close()

	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	count, err := (Client{BaseURL: server.URL, Version: "2022-06-28", Token: "secret", HTTP: http.DefaultClient}).ingestComments(context.Background(), st, "page1", "")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("unexpected comment count: %d", count)
	}
}

func TestIngestCommentsRetriesTransientGatewayError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/comments" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"retryable":true,"retry_after":0}`))
			return
		}
		_, _ = w.Write([]byte(`{
			"object":"list",
			"results":[{
				"id":"comment1",
				"rich_text":[{"type":"text","plain_text":"Looks good","text":{"content":"Looks good"}}],
				"created_by":{"id":"user1"},
				"created_time":"2026-01-01T00:00:00Z",
				"last_edited_time":"2026-01-01T00:00:00Z"
			}],
			"has_more":false
		}`))
	}))
	defer server.Close()

	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	count, err := (Client{BaseURL: server.URL, Version: "2026-03-11", Token: "secret", HTTP: http.DefaultClient}).ingestComments(context.Background(), st, "page1", "space1")
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || attempts != 2 {
		t.Fatalf("unexpected count/attempts: count=%d attempts=%d", count, attempts)
	}
	comments, err := st.PageComments(context.Background(), "page1")
	if err != nil {
		t.Fatal(err)
	}
	if len(comments) != 1 || comments[0].Text != "Looks good" {
		t.Fatalf("unexpected comments: %+v", comments)
	}
}
