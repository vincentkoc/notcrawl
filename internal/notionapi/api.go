package notionapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/vincentkoc/notcrawl/internal/notiontext"
	"github.com/vincentkoc/notcrawl/internal/store"
)

const SourceName = "api"

type Client struct {
	BaseURL string
	Version string
	Token   string
	HTTP    *http.Client
}

type Summary struct {
	Users        int
	Pages        int
	Blocks       int
	Comments     int
	Databases    int
	DatabaseRows int
}

func (c Client) Sync(ctx context.Context, st *store.Store) (Summary, error) {
	if strings.TrimSpace(c.Token) == "" {
		return Summary{}, fmt.Errorf("missing Notion API token")
	}
	if c.BaseURL == "" {
		c.BaseURL = "https://api.notion.com/v1"
	}
	if c.Version == "" {
		c.Version = "2026-03-11"
	}
	if c.HTTP == nil {
		c.HTTP = http.DefaultClient
	}
	var s Summary
	if err := st.DeferPageFTS(ctx, func() error {
		users, err := c.listUsers(ctx)
		if err != nil {
			return err
		}
		for _, u := range users {
			raw := notiontext.MarshalRaw(u)
			if err := st.UpsertUser(ctx, store.User{
				ID: u.string("id"), Name: userName(u), Email: userEmail(u), RawJSON: raw, Source: SourceName, SyncedAt: store.NowMS(),
			}); err != nil {
				return err
			}
			s.Users++
		}
		pages, err := c.searchPages(ctx)
		if err != nil {
			return err
		}
		for _, page := range pages {
			count, comments, err := c.ingestPage(ctx, st, page, ingestPageOptions{FetchBlocks: true, FetchComments: true})
			if err != nil {
				return err
			}
			s.Pages++
			s.Blocks += count
			s.Comments += comments
		}
		collections, err := c.searchCollections(ctx)
		if err != nil {
			return err
		}
		for _, collection := range collections {
			rows, err := c.ingestCollection(ctx, st, collection)
			if err != nil {
				return err
			}
			s.Databases++
			s.DatabaseRows += rows
		}
		if err := st.SetSyncState(ctx, SourceName, "workspace", "default", time.Now().Format(time.RFC3339)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return s, err
	}
	return s, nil
}

type obj map[string]any

func (o obj) string(key string) string {
	if v, ok := o[key].(string); ok {
		return v
	}
	return ""
}

func (o obj) bool(key string) bool {
	if v, ok := o[key].(bool); ok {
		return v
	}
	return false
}

func (o obj) mapObj(key string) obj {
	if v, ok := o[key].(map[string]any); ok {
		return obj(v)
	}
	return nil
}

func (c Client) listUsers(ctx context.Context) ([]obj, error) {
	var out []obj
	cursor := ""
	for {
		path := "/users?page_size=100"
		if cursor != "" {
			path += "&start_cursor=" + url.QueryEscape(cursor)
		}
		var resp obj
		if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
			return nil, err
		}
		for _, item := range asSlice(resp["results"]) {
			if m, ok := item.(map[string]any); ok {
				out = append(out, obj(m))
			}
		}
		if !truthy(resp["has_more"]) {
			return out, nil
		}
		cursor, _ = resp["next_cursor"].(string)
		if cursor == "" {
			return out, nil
		}
	}
}

func (c Client) searchPages(ctx context.Context) ([]obj, error) {
	return c.searchObjects(ctx, "page")
}

func (c Client) searchCollections(ctx context.Context) ([]obj, error) {
	return c.searchObjects(ctx, c.collectionSearchType())
}

func (c Client) searchObjects(ctx context.Context, objectType string) ([]obj, error) {
	var out []obj
	cursor := ""
	for {
		body := obj{"page_size": 100, "filter": obj{"property": "object", "value": objectType}}
		if cursor != "" {
			body["start_cursor"] = cursor
		}
		var resp obj
		if err := c.do(ctx, http.MethodPost, "/search", body, &resp); err != nil {
			return nil, err
		}
		for _, item := range asSlice(resp["results"]) {
			if m, ok := item.(map[string]any); ok {
				out = append(out, obj(m))
			}
		}
		if !truthy(resp["has_more"]) {
			return out, nil
		}
		cursor, _ = resp["next_cursor"].(string)
		if cursor == "" {
			return out, nil
		}
	}
}

type ingestPageOptions struct {
	CollectionID  string
	FetchBlocks   bool
	FetchComments bool
}

func (c Client) ingestPage(ctx context.Context, st *store.Store, page obj, opts ingestPageOptions) (blockCount int, commentCount int, err error) {
	raw := notiontext.MarshalRaw(page)
	props := marshalAny(page["properties"])
	parent := page.mapObj("parent")
	parentID := parent.string("page_id")
	if parentID == "" {
		parentID = parent.string("database_id")
	}
	if parentID == "" {
		parentID = parent.string("data_source_id")
	}
	collectionID := opts.CollectionID
	if collectionID == "" && (parent.string("type") == "database_id" || parent.string("type") == "data_source_id") {
		collectionID = parentID
	}
	spaceID := parent.string("workspace")
	p := store.Page{
		ID:             page.string("id"),
		SpaceID:        spaceID,
		ParentID:       parentID,
		ParentTable:    parent.string("type"),
		CollectionID:   collectionID,
		Title:          titleFromAPIPage(page),
		URL:            page.string("url"),
		PropertiesJSON: props,
		CreatedTime:    parseTimeMS(page.string("created_time")),
		LastEditedTime: parseTimeMS(page.string("last_edited_time")),
		Alive:          !page.bool("archived") && !page.bool("in_trash"),
		Source:         SourceName,
		RawJSON:        raw,
		SyncedAt:       store.NowMS(),
	}
	if p.Title == "" {
		p.Title = "Untitled"
	}
	if err := st.UpsertPage(ctx, p); err != nil {
		return 0, 0, err
	}
	var blocks, comments int
	if opts.FetchBlocks {
		blocks, err = c.walkBlocks(ctx, st, p.ID, p.ID, p.SpaceID)
		if err != nil {
			return 0, 0, err
		}
	}
	if opts.FetchComments {
		comments, err = c.ingestComments(ctx, st, p.ID, p.SpaceID)
		if err != nil {
			return 0, 0, err
		}
	}
	return blocks, comments, nil
}

func (c Client) ingestCollection(ctx context.Context, st *store.Store, collection obj) (int, error) {
	id := collection.string("id")
	raw := notiontext.MarshalRaw(collection)
	parent := collection.mapObj("parent")
	if len(parent) == 0 {
		parent = collection.mapObj("database_parent")
	}
	parentID := firstNonEmpty(parent.string("database_id"), parent.string("page_id"), parent.string("block_id"), parent.string("workspace"))
	name := notiontext.Plain(collection["title"])
	if name == "" {
		name = id
	}
	if err := st.UpsertCollection(ctx, store.Collection{
		ID:          id,
		SpaceID:     parent.string("workspace"),
		ParentID:    parentID,
		ParentTable: parent.string("type"),
		Name:        name,
		SchemaJSON:  marshalAny(collection["properties"]),
		FormatJSON:  marshalAny(collection),
		RawJSON:     raw,
		Source:      SourceName,
		SyncedAt:    store.NowMS(),
	}); err != nil {
		return 0, err
	}
	if err := st.UpsertRawRecord(ctx, store.RawRecord{
		Source: SourceName, RecordTable: c.collectionSearchType(), RecordID: id, ParentID: parentID,
		SpaceID: parent.string("workspace"), RawJSON: raw, SyncedAt: store.NowMS(),
	}); err != nil {
		return 0, err
	}
	return c.queryCollection(ctx, st, id)
}

func (c Client) queryCollection(ctx context.Context, st *store.Store, collectionID string) (int, error) {
	var count int
	cursor := ""
	for {
		body := obj{"page_size": 100}
		if cursor != "" {
			body["start_cursor"] = cursor
		}
		var resp obj
		path := fmt.Sprintf("%s/%s/query", c.collectionQueryBasePath(), url.PathEscape(collectionID))
		if err := c.do(ctx, http.MethodPost, path, body, &resp); err != nil {
			return count, err
		}
		for _, item := range asSlice(resp["results"]) {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if itemType := obj(m).string("object"); itemType != "" && itemType != "page" {
				if itemType == c.collectionSearchType() {
					if _, err := c.ingestCollection(ctx, st, obj(m)); err != nil {
						return count, err
					}
				}
				continue
			}
			if _, _, err := c.ingestPage(ctx, st, obj(m), ingestPageOptions{CollectionID: collectionID}); err != nil {
				return count, err
			}
			count++
		}
		if !truthy(resp["has_more"]) {
			return count, nil
		}
		cursor, _ = resp["next_cursor"].(string)
		if cursor == "" {
			return count, nil
		}
	}
}

func (c Client) collectionSearchType() string {
	if c.usesDataSourceAPI() {
		return "data_source"
	}
	return "database"
}

func (c Client) collectionQueryBasePath() string {
	if c.usesDataSourceAPI() {
		return "/data_sources"
	}
	return "/databases"
}

func (c Client) usesDataSourceAPI() bool {
	return c.Version >= "2025-09-03"
}

func (c Client) walkBlocks(ctx context.Context, st *store.Store, pageID, parentID, spaceID string) (int, error) {
	var count int
	cursor := ""
	var displayOrder int64
	for {
		path := fmt.Sprintf("/blocks/%s/children?page_size=100", url.PathEscape(parentID))
		if cursor != "" {
			path += "&start_cursor=" + url.QueryEscape(cursor)
		}
		var resp obj
		if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
			return count, err
		}
		for _, item := range asSlice(resp["results"]) {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			block := obj(m)
			typ := block.string("type")
			typeBody := block[typ]
			text := notiontext.Plain(typeBody)
			raw := notiontext.MarshalRaw(block)
			displayOrder++
			if err := st.UpsertBlock(ctx, store.Block{
				ID:             block.string("id"),
				PageID:         pageID,
				SpaceID:        spaceID,
				ParentID:       parentID,
				ParentTable:    "block",
				Type:           typ,
				Text:           text,
				PropertiesJSON: marshalAny(typeBody),
				DisplayOrder:   displayOrder,
				CreatedTime:    parseTimeMS(block.string("created_time")),
				LastEditedTime: parseTimeMS(block.string("last_edited_time")),
				Alive:          !block.bool("archived") && !block.bool("in_trash"),
				Source:         SourceName,
				RawJSON:        raw,
				SyncedAt:       store.NowMS(),
			}); err != nil {
				return count, err
			}
			count++
			if truthy(block["has_children"]) {
				n, err := c.walkBlocks(ctx, st, pageID, block.string("id"), spaceID)
				if err != nil {
					return count, err
				}
				count += n
			}
		}
		if !truthy(resp["has_more"]) {
			return count, nil
		}
		cursor, _ = resp["next_cursor"].(string)
		if cursor == "" {
			return count, nil
		}
	}
}

func (c Client) ingestComments(ctx context.Context, st *store.Store, pageID, spaceID string) (int, error) {
	var count int
	cursor := ""
	for {
		path := "/comments?block_id=" + url.QueryEscape(pageID) + "&page_size=100"
		if cursor != "" {
			path += "&start_cursor=" + url.QueryEscape(cursor)
		}
		var resp obj
		if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
			if isIgnoredCommentError(err) {
				return count, nil
			}
			return count, err
		}
		for _, item := range asSlice(resp["results"]) {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			comment := obj(m)
			createdBy := comment.mapObj("created_by")
			if err := st.UpsertComment(ctx, store.Comment{
				ID:             comment.string("id"),
				PageID:         pageID,
				SpaceID:        spaceID,
				ParentID:       pageID,
				Text:           notiontext.Plain(comment["rich_text"]),
				CreatedByID:    createdBy.string("id"),
				CreatedTime:    parseTimeMS(comment.string("created_time")),
				LastEditedTime: parseTimeMS(comment.string("last_edited_time")),
				Alive:          true,
				RawJSON:        notiontext.MarshalRaw(comment),
				Source:         SourceName,
				SyncedAt:       store.NowMS(),
			}); err != nil {
				return count, err
			}
			count++
		}
		if !truthy(resp["has_more"]) {
			return count, nil
		}
		cursor, _ = resp["next_cursor"].(string)
		if cursor == "" {
			return count, nil
		}
	}
}

func (c Client) do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Notion-Version", c.Version)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		if wait, err := time.ParseDuration(resp.Header.Get("Retry-After") + "s"); err == nil && wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			return c.do(ctx, method, path, body, out)
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		bodyText := strings.TrimSpace(string(b))
		apiErr := notionAPIError{Method: method, Path: path, Status: resp.Status, StatusCode: resp.StatusCode, Body: bodyText}
		var payload struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(b, &payload); err == nil {
			apiErr.Code = payload.Code
			apiErr.Message = payload.Message
		}
		return apiErr
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type notionAPIError struct {
	Method     string
	Path       string
	Status     string
	StatusCode int
	Code       string
	Message    string
	Body       string
}

func (e notionAPIError) Error() string {
	if e.Code != "" || e.Message != "" {
		return fmt.Sprintf("notion api %s %s: %s: %s: %s", e.Method, e.Path, e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("notion api %s %s: %s: %s", e.Method, e.Path, e.Status, e.Body)
}

func isIgnoredCommentError(err error) bool {
	apiErr, ok := err.(notionAPIError)
	if !ok {
		return false
	}
	if apiErr.StatusCode == http.StatusNotFound || apiErr.Code == "not_found" {
		return true
	}
	return apiErr.StatusCode == http.StatusForbidden && apiErr.Code == "restricted_resource"
}

func userName(u obj) string {
	if name := u.string("name"); name != "" {
		return name
	}
	person := u.mapObj("person")
	return person.string("email")
}

func userEmail(u obj) string {
	person := u.mapObj("person")
	return person.string("email")
}

func titleFromAPIPage(page obj) string {
	props, ok := page["properties"].(map[string]any)
	if !ok {
		return ""
	}
	for _, prop := range props {
		m, ok := prop.(map[string]any)
		if !ok || m["type"] != "title" {
			continue
		}
		return notiontext.Plain(m["title"])
	}
	return ""
}

func marshalAny(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func parseTimeMS(s string) int64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

func truthy(v any) bool {
	b, _ := v.(bool)
	return b
}

func asSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
