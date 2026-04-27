package notiondesktop

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vincentkoc/notcrawl/internal/notiontext"
	"github.com/vincentkoc/notcrawl/internal/store"
	_ "modernc.org/sqlite"
)

const SourceName = "desktop"

type Source struct {
	Path      string
	Snapshot  string
	Available bool
	SizeBytes int64
}

type Summary struct {
	Source      Source
	Spaces      int
	Users       int
	Teams       int
	Pages       int
	Blocks      int
	Collections int
	Comments    int
	RawRecords  int
}

func Inspect(path string) (Source, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Source{Path: path, Available: false}, nil
	}
	if info.IsDir() {
		return Source{Path: path, Available: false}, fmt.Errorf("desktop path is a directory, expected notion.db: %s", path)
	}
	return Source{Path: path, Available: true, SizeBytes: info.Size()}, nil
}

func Ingest(ctx context.Context, st *store.Store, path, cacheDir string) (Summary, error) {
	source, err := Inspect(path)
	if err != nil || !source.Available {
		return Summary{Source: source}, err
	}
	snapshot, err := snapshotDB(path, cacheDir)
	if err != nil {
		return Summary{Source: source}, err
	}
	source.Snapshot = snapshot
	db, err := sql.Open("sqlite", snapshot)
	if err != nil {
		return Summary{Source: source}, err
	}
	defer db.Close()
	s := Summary{Source: source}
	if err := st.DeferPageFTS(ctx, func() error {
		if s.Spaces, err = ingestSpaces(ctx, st, db); err != nil {
			return err
		}
		if s.Users, err = ingestUsers(ctx, st, db); err != nil {
			return err
		}
		if s.Teams, err = ingestTeams(ctx, st, db); err != nil {
			return err
		}
		if s.Collections, err = ingestCollections(ctx, st, db); err != nil {
			return err
		}
		if s.Pages, s.Blocks, s.RawRecords, err = ingestBlocks(ctx, st, db); err != nil {
			return err
		}
		if s.Comments, err = ingestComments(ctx, st, db); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return s, err
	}
	if err := st.SetSyncState(ctx, SourceName, "desktop", "notion.db", snapshot); err != nil {
		return s, err
	}
	return s, nil
}

func snapshotDB(path, cacheDir string) (string, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", err
	}
	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer in.Close()
	outPath := filepath.Join(cacheDir, fmt.Sprintf("notion-desktop-%d.db", time.Now().UnixMilli()))
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return "", err
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		src := path + suffix
		if _, err := os.Stat(src); err == nil {
			if err := copyFile(src, outPath+suffix, 0o600); err != nil {
				return "", err
			}
		} else if !os.IsNotExist(err) {
			return "", err
		}
	}
	return outPath, nil
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func ingestSpaces(ctx context.Context, st *store.Store, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, `select id, coalesce(name, ''), coalesce(json_object(
		'id', id, 'name', name, 'pages', pages, 'settings', settings, 'created_time', created_time, 'last_edited_time', last_edited_time
	), '{}') from space`)
	if err != nil {
		return 0, ignoreMissingTable(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id, name, raw string
		if err := rows.Scan(&id, &name, &raw); err != nil {
			return n, err
		}
		if name == "" {
			name = id
		}
		if err := st.UpsertSpace(ctx, store.Space{ID: id, Name: name, RawJSON: raw, Source: SourceName, SyncedAt: store.NowMS()}); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

func ingestUsers(ctx context.Context, st *store.Store, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, `select id, coalesce(name, ''), coalesce(email, ''), coalesce(json_object(
		'id', id, 'name', name, 'email', email, 'given_name', given_name, 'family_name', family_name, 'profile_photo', profile_photo
	), '{}') from notion_user`)
	if err != nil {
		return 0, ignoreMissingTable(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id, name, email, raw string
		if err := rows.Scan(&id, &name, &email, &raw); err != nil {
			return n, err
		}
		if err := st.UpsertUser(ctx, store.User{ID: id, Name: name, Email: email, RawJSON: raw, Source: SourceName, SyncedAt: store.NowMS()}); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

func ingestTeams(ctx context.Context, st *store.Store, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, `select id, space_id, parent_id, parent_table, coalesce(name, ''),
		coalesce(json_object('id', id, 'space_id', space_id, 'parent_id', parent_id, 'parent_table', parent_table,
			'name', name, 'description', description, 'team_pages', team_pages, 'settings', settings), '{}')
		from team where coalesce(archived_at, 0) = 0`)
	if err != nil {
		return 0, ignoreMissingTable(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var x store.Team
		if err := rows.Scan(&x.ID, &x.SpaceID, &x.ParentID, &x.ParentTable, &x.Name, &x.RawJSON); err != nil {
			return n, err
		}
		if x.Name == "" {
			x.Name = x.ID
		}
		x.Source = SourceName
		x.SyncedAt = store.NowMS()
		if err := st.UpsertTeam(ctx, x); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

func ingestCollections(ctx context.Context, st *store.Store, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, `select id, space_id, parent_id, parent_table, coalesce(name, ''), coalesce(schema, ''), coalesce(format, ''),
		coalesce(json_object('id', id, 'space_id', space_id, 'parent_id', parent_id, 'parent_table', parent_table,
			'name', name, 'schema', schema, 'format', format), '{}')
		from collection where alive = 1`)
	if err != nil {
		return 0, ignoreMissingTable(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var x store.Collection
		if err := rows.Scan(&x.ID, &x.SpaceID, &x.ParentID, &x.ParentTable, &x.Name, &x.SchemaJSON, &x.FormatJSON, &x.RawJSON); err != nil {
			return n, err
		}
		x.Name = notiontext.TitleFromProperties(x.Name)
		if x.Name == "" {
			x.Name = x.ID
		}
		x.Source = SourceName
		x.SyncedAt = store.NowMS()
		if err := st.UpsertCollection(ctx, x); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

type localBlock struct {
	ID             string
	SpaceID        string
	Type           string
	PropertiesJSON string
	ContentJSON    string
	CollectionID   string
	CreatedTime    int64
	LastEditedTime int64
	ParentID       string
	ParentTable    string
	Alive          bool
	FormatJSON     string
	RawJSON        string
}

func ingestBlocks(ctx context.Context, st *store.Store, db *sql.DB) (pages int, blocks int, rawRecords int, err error) {
	rows, err := db.QueryContext(ctx, `select id, space_id, type, coalesce(properties, ''), coalesce(content, ''),
		coalesce(collection_id, ''), coalesce(cast(created_time as integer), 0), coalesce(cast(last_edited_time as integer), 0),
		coalesce(parent_id, ''), coalesce(parent_table, ''), alive, coalesce(format, ''),
		coalesce(json_object('id', id, 'space_id', space_id, 'type', type, 'properties', properties, 'content', content,
			'collection_id', collection_id, 'created_time', created_time, 'last_edited_time', last_edited_time,
			'parent_id', parent_id, 'parent_table', parent_table, 'alive', alive, 'format', format), '{}')
		from block`)
	if err != nil {
		return 0, 0, 0, ignoreMissingTable(err)
	}
	defer rows.Close()
	byID := map[string]localBlock{}
	var all []localBlock
	for rows.Next() {
		var b localBlock
		var alive int
		if err := rows.Scan(&b.ID, &b.SpaceID, &b.Type, &b.PropertiesJSON, &b.ContentJSON, &b.CollectionID, &b.CreatedTime,
			&b.LastEditedTime, &b.ParentID, &b.ParentTable, &alive, &b.FormatJSON, &b.RawJSON); err != nil {
			return pages, blocks, rawRecords, err
		}
		b.Alive = alive != 0
		byID[b.ID] = b
		all = append(all, b)
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, err
	}
	pageFor := func(id string) string { return "" }
	var resolve func(string, map[string]bool) string
	resolve = func(id string, seen map[string]bool) string {
		if id == "" || seen[id] {
			return ""
		}
		seen[id] = true
		b, ok := byID[id]
		if !ok {
			return ""
		}
		if isPageType(b.Type) {
			return b.ID
		}
		if b.ParentTable == "block" {
			return resolve(b.ParentID, seen)
		}
		return ""
	}
	pageFor = func(id string) string { return resolve(id, map[string]bool{}) }
	for _, b := range all {
		title := notiontext.TitleFromProperties(b.PropertiesJSON)
		if title == "" && isPageType(b.Type) {
			title = "Untitled"
		}
		if isPageType(b.Type) {
			if err := st.UpsertPage(ctx, store.Page{
				ID:             b.ID,
				SpaceID:        b.SpaceID,
				ParentID:       b.ParentID,
				ParentTable:    b.ParentTable,
				CollectionID:   b.CollectionID,
				Title:          title,
				PropertiesJSON: b.PropertiesJSON,
				CreatedTime:    b.CreatedTime,
				LastEditedTime: b.LastEditedTime,
				Alive:          b.Alive,
				Source:         SourceName,
				RawJSON:        b.RawJSON,
				SyncedAt:       store.NowMS(),
			}); err != nil {
				return pages, blocks, rawRecords, err
			}
			pages++
		}
		pageID := pageFor(b.ID)
		text := notiontext.PlainFromJSON(b.PropertiesJSON)
		if err := st.UpsertBlock(ctx, store.Block{
			ID:             b.ID,
			PageID:         pageID,
			SpaceID:        b.SpaceID,
			ParentID:       b.ParentID,
			ParentTable:    b.ParentTable,
			Type:           b.Type,
			Text:           text,
			PropertiesJSON: b.PropertiesJSON,
			ContentJSON:    b.ContentJSON,
			FormatJSON:     b.FormatJSON,
			CreatedTime:    b.CreatedTime,
			LastEditedTime: b.LastEditedTime,
			Alive:          b.Alive,
			Source:         SourceName,
			RawJSON:        b.RawJSON,
			SyncedAt:       store.NowMS(),
		}); err != nil {
			return pages, blocks, rawRecords, err
		}
		blocks++
		if err := st.UpsertRawRecord(ctx, store.RawRecord{
			Source: SourceName, RecordTable: "block", RecordID: b.ID, ParentID: b.ParentID,
			SpaceID: b.SpaceID, RawJSON: b.RawJSON, SyncedAt: store.NowMS(),
		}); err != nil {
			return pages, blocks, rawRecords, err
		}
		rawRecords++
	}
	return pages, blocks, rawRecords, nil
}

func ingestComments(ctx context.Context, st *store.Store, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, `select id, parent_id, space_id, coalesce(text, ''), coalesce(created_by_id, ''),
		coalesce(cast(created_time as integer), 0), coalesce(cast(last_edited_time as integer), 0), alive,
		coalesce(json_object('id', id, 'parent_id', parent_id, 'space_id', space_id, 'text', text, 'content', content,
			'created_by_id', created_by_id, 'created_time', created_time, 'last_edited_time', last_edited_time, 'alive', alive), '{}')
		from comment`)
	if err != nil {
		return 0, ignoreMissingTable(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var c store.Comment
		var alive int
		if err := rows.Scan(&c.ID, &c.ParentID, &c.SpaceID, &c.Text, &c.CreatedByID, &c.CreatedTime, &c.LastEditedTime, &alive, &c.RawJSON); err != nil {
			return n, err
		}
		c.PageID = c.ParentID
		c.Text = notiontext.PlainFromJSON(c.Text)
		c.Alive = alive != 0
		c.Source = SourceName
		c.SyncedAt = store.NowMS()
		if err := st.UpsertComment(ctx, c); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

func isPageType(t string) bool {
	return t == "page" || t == "collection_view_page" || t == "external_object_instance_page"
}

func ignoreMissingTable(err error) error {
	if err != nil && strings.Contains(err.Error(), "no such table") {
		return nil
	}
	return err
}
