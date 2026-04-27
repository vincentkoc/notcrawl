package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 1

type Store struct {
	db               *sql.DB
	path             string
	deferredFTS      int
	deferredFTSPages map[string]bool
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := ensureDBFile(path); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	st := &Store{db: db, path: path}
	if err := st.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func sqliteDSN(path string) string {
	pragmas := "_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=temp_store(MEMORY)&_pragma=mmap_size(268435456)&_pragma=busy_timeout(5000)"
	if path == ":memory:" {
		return "file::memory:?cache=shared&" + pragmas
	}
	if strings.HasPrefix(path, "file:") {
		sep := "?"
		if strings.Contains(path, "?") {
			sep = "&"
		}
		return path + sep + pragmas
	}
	return "file:" + path + "?" + pragmas
}

func ensureDBFile(path string) error {
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil
	}
	if _, err := os.Stat(path); err == nil {
		return os.Chmod(path, 0o600)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if file != nil {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

type Status struct {
	DBPath      string `json:"db_path"`
	DBBytes     int64  `json:"db_bytes"`
	WALBytes    int64  `json:"wal_bytes"`
	Spaces      int    `json:"spaces"`
	Users       int    `json:"users"`
	Teams       int    `json:"teams"`
	Pages       int    `json:"pages"`
	Blocks      int    `json:"blocks"`
	Collections int    `json:"collections"`
	Comments    int    `json:"comments"`
	RawRecords  int    `json:"raw_records"`
	LastSyncAt  int64  `json:"last_sync_at"`
}

type MaintenanceSummary struct {
	RebuiltFTS bool   `json:"rebuilt_fts"`
	Optimized  bool   `json:"optimized"`
	Analyzed   bool   `json:"analyzed"`
	Vacuumed   bool   `json:"vacuumed"`
	DBBytes    int64  `json:"db_bytes"`
	WALBytes   int64  `json:"wal_bytes"`
	Message    string `json:"message"`
}

func (s *Store) init(ctx context.Context) error {
	stmts := []string{
		`pragma foreign_keys = on`,
		`pragma journal_mode = wal`,
		`pragma synchronous = normal`,
		`pragma temp_store = memory`,
		`pragma mmap_size = 268435456`,
		`pragma busy_timeout = 5000`,
		`create table if not exists meta (key text primary key, value text not null)`,
		`create table if not exists spaces (
			id text primary key,
			name text not null,
			raw_json text,
			source text not null,
			synced_at integer not null
		)`,
		`create table if not exists users (
			id text primary key,
			name text,
			email text,
			raw_json text,
			source text not null,
			synced_at integer not null
		)`,
		`create table if not exists teams (
			id text primary key,
			space_id text,
			parent_id text,
			parent_table text,
			name text not null,
			raw_json text,
			source text not null,
			synced_at integer not null
		)`,
		`create index if not exists teams_space_id on teams(space_id)`,
		`create table if not exists pages (
			id text primary key,
			space_id text,
			parent_id text,
			parent_table text,
			collection_id text,
			title text not null,
			url text,
			icon text,
			cover text,
			properties_json text,
			created_time integer,
			last_edited_time integer,
			alive integer not null,
			source text not null,
			raw_json text,
			synced_at integer not null
		)`,
		`create index if not exists pages_collection_id on pages(collection_id)`,
		`create index if not exists pages_parent_id on pages(parent_id)`,
		`create index if not exists pages_last_edited_time on pages(last_edited_time desc)`,
		`create index if not exists pages_source_synced_at on pages(source, synced_at desc)`,
		`create table if not exists blocks (
			id text primary key,
			page_id text,
			space_id text,
			parent_id text,
			parent_table text,
			type text not null,
			text text,
			properties_json text,
			content_json text,
			format_json text,
			display_order integer not null default 0,
			created_time integer,
			last_edited_time integer,
			alive integer not null,
			source text not null,
			raw_json text,
			synced_at integer not null
		)`,
		`create index if not exists blocks_page_id on blocks(page_id)`,
		`create index if not exists blocks_parent_id on blocks(parent_id)`,
		`create table if not exists collections (
			id text primary key,
			space_id text,
			parent_id text,
			parent_table text,
			name text,
			schema_json text,
			format_json text,
			raw_json text,
			source text not null,
			synced_at integer not null
		)`,
		`create index if not exists collections_parent_id on collections(parent_id)`,
		`create index if not exists collections_name on collections(name)`,
		`create table if not exists comments (
			id text primary key,
			page_id text,
			space_id text,
			parent_id text,
			text text,
			created_by_id text,
			created_time integer,
			last_edited_time integer,
			alive integer not null,
			raw_json text,
			source text not null,
			synced_at integer not null
		)`,
		`create index if not exists comments_page_id on comments(page_id)`,
		`create index if not exists comments_created_time on comments(created_time, id)`,
		`create table if not exists raw_records (
			source text not null,
			record_table text not null,
			record_id text not null,
			parent_id text,
			space_id text,
			raw_json text not null,
			synced_at integer not null,
			primary key (source, record_table, record_id)
		)`,
		`create index if not exists raw_records_parent on raw_records(parent_id, record_table)`,
		`create table if not exists sync_state (
			source text not null,
			entity_type text not null,
			entity_id text not null,
			cursor text,
			synced_at integer not null,
			primary key (source, entity_type, entity_id)
		)`,
		`create index if not exists sync_state_synced_at on sync_state(synced_at desc)`,
		`create virtual table if not exists page_fts using fts5(page_id unindexed, title, body)`,
		`create virtual table if not exists comment_fts using fts5(comment_id unindexed, page_id unindexed, body)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	var current int
	row := s.db.QueryRowContext(ctx, `select value from meta where key = 'schema_version'`)
	err := row.Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if current > schemaVersion {
		return fmt.Errorf("database schema version %d is newer than this notcrawl build supports (%d)", current, schemaVersion)
	}
	if err := s.ensureColumn(ctx, "blocks", "display_order", "integer not null default 0"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "collections", "parent_table", "text"); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `create index if not exists blocks_page_alive_order on blocks(page_id, alive, parent_id, display_order, created_time, id)`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `create index if not exists blocks_page_alive_created on blocks(page_id, alive, created_time, id)`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `insert or replace into meta(key, value) values('schema_version', ?)`, schemaVersion); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, definition string) error {
	rows, err := s.db.QueryContext(ctx, `pragma table_info(`+table+`)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == column {
			return rows.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `alter table `+table+` add column `+column+` `+definition)
	return err
}

func NowMS() int64 {
	return time.Now().UnixMilli()
}

func BoolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func IntBool(v int) bool {
	return v != 0
}

func (s *Store) UpsertSpace(ctx context.Context, x Space) error {
	_, err := s.db.ExecContext(ctx, `insert into spaces(id, name, raw_json, source, synced_at)
		values (?, ?, ?, ?, ?)
		on conflict(id) do update set name=excluded.name, raw_json=excluded.raw_json, source=excluded.source, synced_at=excluded.synced_at`,
		x.ID, x.Name, x.RawJSON, x.Source, x.SyncedAt)
	return err
}

func (s *Store) UpsertUser(ctx context.Context, x User) error {
	_, err := s.db.ExecContext(ctx, `insert into users(id, name, email, raw_json, source, synced_at)
		values (?, ?, ?, ?, ?, ?)
		on conflict(id) do update set name=excluded.name, email=excluded.email, raw_json=excluded.raw_json, source=excluded.source, synced_at=excluded.synced_at`,
		x.ID, x.Name, x.Email, x.RawJSON, x.Source, x.SyncedAt)
	return err
}

func (s *Store) UpsertTeam(ctx context.Context, x Team) error {
	_, err := s.db.ExecContext(ctx, `insert into teams(id, space_id, parent_id, parent_table, name, raw_json, source, synced_at)
		values (?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(id) do update set
			space_id=excluded.space_id,
			parent_id=excluded.parent_id,
			parent_table=excluded.parent_table,
			name=excluded.name,
			raw_json=excluded.raw_json,
			source=excluded.source,
			synced_at=excluded.synced_at`,
		x.ID, x.SpaceID, x.ParentID, x.ParentTable, x.Name, x.RawJSON, x.Source, x.SyncedAt)
	return err
}

func (s *Store) UpsertPage(ctx context.Context, x Page) error {
	_, err := s.db.ExecContext(ctx, `insert into pages(
		id, space_id, parent_id, parent_table, collection_id, title, url, icon, cover, properties_json,
		created_time, last_edited_time, alive, source, raw_json, synced_at)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(id) do update set
			space_id=excluded.space_id,
			parent_id=excluded.parent_id,
			parent_table=excluded.parent_table,
			collection_id=excluded.collection_id,
			title=excluded.title,
			url=excluded.url,
			icon=excluded.icon,
			cover=excluded.cover,
			properties_json=excluded.properties_json,
			created_time=excluded.created_time,
			last_edited_time=excluded.last_edited_time,
			alive=excluded.alive,
			source=excluded.source,
			raw_json=excluded.raw_json,
			synced_at=excluded.synced_at`,
		x.ID, x.SpaceID, x.ParentID, x.ParentTable, x.CollectionID, x.Title, x.URL, x.Icon, x.Cover, x.PropertiesJSON,
		x.CreatedTime, x.LastEditedTime, BoolInt(x.Alive), x.Source, x.RawJSON, x.SyncedAt)
	if err != nil {
		return err
	}
	return s.markPageFTS(ctx, x.ID)
}

func (s *Store) UpsertBlock(ctx context.Context, x Block) error {
	_, err := s.db.ExecContext(ctx, `insert into blocks(
		id, page_id, space_id, parent_id, parent_table, type, text, properties_json, content_json, format_json,
		display_order, created_time, last_edited_time, alive, source, raw_json, synced_at)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(id) do update set
			page_id=excluded.page_id,
			space_id=excluded.space_id,
			parent_id=excluded.parent_id,
			parent_table=excluded.parent_table,
			type=excluded.type,
			text=excluded.text,
			properties_json=excluded.properties_json,
			content_json=excluded.content_json,
			format_json=excluded.format_json,
			display_order=excluded.display_order,
			created_time=excluded.created_time,
			last_edited_time=excluded.last_edited_time,
			alive=excluded.alive,
			source=excluded.source,
			raw_json=excluded.raw_json,
			synced_at=excluded.synced_at`,
		x.ID, x.PageID, x.SpaceID, x.ParentID, x.ParentTable, x.Type, x.Text, x.PropertiesJSON, x.ContentJSON, x.FormatJSON,
		x.DisplayOrder, x.CreatedTime, x.LastEditedTime, BoolInt(x.Alive), x.Source, x.RawJSON, x.SyncedAt)
	if err != nil {
		return err
	}
	if x.PageID != "" {
		return s.markPageFTS(ctx, x.PageID)
	}
	return nil
}

func (s *Store) UpsertCollection(ctx context.Context, x Collection) error {
	_, err := s.db.ExecContext(ctx, `insert into collections(id, space_id, parent_id, parent_table, name, schema_json, format_json, raw_json, source, synced_at)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(id) do update set space_id=excluded.space_id, parent_id=excluded.parent_id, parent_table=excluded.parent_table, name=excluded.name,
			schema_json=excluded.schema_json, format_json=excluded.format_json, raw_json=excluded.raw_json,
			source=excluded.source, synced_at=excluded.synced_at`,
		x.ID, x.SpaceID, x.ParentID, x.ParentTable, x.Name, x.SchemaJSON, x.FormatJSON, x.RawJSON, x.Source, x.SyncedAt)
	return err
}

func (s *Store) UpsertComment(ctx context.Context, x Comment) error {
	_, err := s.db.ExecContext(ctx, `insert into comments(id, page_id, space_id, parent_id, text, created_by_id, created_time, last_edited_time, alive, raw_json, source, synced_at)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(id) do update set page_id=excluded.page_id, space_id=excluded.space_id, parent_id=excluded.parent_id,
			text=excluded.text, created_by_id=excluded.created_by_id, created_time=excluded.created_time,
			last_edited_time=excluded.last_edited_time, alive=excluded.alive, raw_json=excluded.raw_json,
			source=excluded.source, synced_at=excluded.synced_at`,
		x.ID, x.PageID, x.SpaceID, x.ParentID, x.Text, x.CreatedByID, x.CreatedTime, x.LastEditedTime, BoolInt(x.Alive), x.RawJSON, x.Source, x.SyncedAt)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `delete from comment_fts where comment_id = ?`, x.ID)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `insert into comment_fts(comment_id, page_id, body) values (?, ?, ?)`, x.ID, x.PageID, x.Text)
	return err
}

func (s *Store) UpsertRawRecord(ctx context.Context, x RawRecord) error {
	_, err := s.db.ExecContext(ctx, `insert into raw_records(source, record_table, record_id, parent_id, space_id, raw_json, synced_at)
		values (?, ?, ?, ?, ?, ?, ?)
		on conflict(source, record_table, record_id) do update set parent_id=excluded.parent_id, space_id=excluded.space_id,
			raw_json=excluded.raw_json, synced_at=excluded.synced_at`,
		x.Source, x.RecordTable, x.RecordID, x.ParentID, x.SpaceID, x.RawJSON, x.SyncedAt)
	return err
}

func (s *Store) SetSyncState(ctx context.Context, source, entityType, entityID, cursor string) error {
	_, err := s.db.ExecContext(ctx, `insert into sync_state(source, entity_type, entity_id, cursor, synced_at)
		values (?, ?, ?, ?, ?)
		on conflict(source, entity_type, entity_id) do update set cursor=excluded.cursor, synced_at=excluded.synced_at`,
		source, entityType, entityID, cursor, NowMS())
	return err
}

func (s *Store) DeferPageFTS(ctx context.Context, fn func() error) error {
	outer := s.deferredFTS == 0
	if outer {
		s.deferredFTSPages = map[string]bool{}
	}
	s.deferredFTS++
	err := fn()
	s.deferredFTS--
	if !outer {
		return err
	}
	pages := s.deferredFTSPages
	s.deferredFTSPages = nil
	if err != nil {
		return err
	}
	for pageID := range pages {
		if err := s.refreshPageFTS(ctx, pageID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) markPageFTS(ctx context.Context, pageID string) error {
	if pageID == "" {
		return nil
	}
	if s.deferredFTS > 0 {
		if s.deferredFTSPages == nil {
			s.deferredFTSPages = map[string]bool{}
		}
		s.deferredFTSPages[pageID] = true
		return nil
	}
	return s.refreshPageFTS(ctx, pageID)
}

func (s *Store) refreshPageFTS(ctx context.Context, pageID string) error {
	var title string
	if err := s.db.QueryRowContext(ctx, `select title from pages where id = ?`, pageID).Scan(&title); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	blocks, err := s.PageBlocks(ctx, pageID)
	if err != nil {
		return err
	}
	parts := pageBlockTextParts(pageID, blocks)
	if _, err := s.db.ExecContext(ctx, `delete from page_fts where page_id = ?`, pageID); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `insert into page_fts(page_id, title, body) values (?, ?, ?)`, pageID, title, strings.Join(parts, "\n"))
	return err
}

func pageBlockTextParts(pageID string, blocks []Block) []string {
	children := map[string][]Block{}
	for _, block := range blocks {
		if block.ID == pageID {
			continue
		}
		children[block.ParentID] = append(children[block.ParentID], block)
	}
	for parent := range children {
		sortBlockSiblings(children[parent])
	}

	var parts []string
	var appendChildren func(string)
	appendChildren = func(parentID string) {
		for _, block := range children[parentID] {
			if strings.TrimSpace(block.Text) != "" {
				parts = append(parts, block.Text)
			}
			appendChildren(block.ID)
		}
	}
	appendChildren(pageID)
	if len(children[pageID]) == 0 {
		for _, block := range blocks {
			if block.ID == pageID || block.ParentID == pageID {
				continue
			}
			if strings.TrimSpace(block.Text) != "" {
				parts = append(parts, block.Text)
			}
		}
	}
	return parts
}

func sortBlockSiblings(blocks []Block) {
	sort.SliceStable(blocks, func(i, j int) bool {
		a, z := blocks[i], blocks[j]
		if a.DisplayOrder != z.DisplayOrder {
			return a.DisplayOrder < z.DisplayOrder
		}
		if a.CreatedTime == z.CreatedTime {
			return a.ID < z.ID
		}
		return a.CreatedTime < z.CreatedTime
	})
}

func (s *Store) Search(ctx context.Context, q string, limit int) ([]SearchResult, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `select 'page', page_id, title, snippet(page_fts, 2, '[', ']', '...', 16)
		from page_fts where page_fts match ? limit ?`, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SearchResult
	for rows.Next() {
		var r SearchResult
		if err := rows.Scan(&r.Kind, &r.ID, &r.Title, &r.Text); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) RebuildFTS(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `delete from page_fts`); err != nil {
		return err
	}
	rows, err := s.db.QueryContext(ctx, `select id from pages`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.refreshPageFTS(ctx, id); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `delete from comment_fts`); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `insert into comment_fts(comment_id, page_id, body) select id, page_id, text from comments where alive = 1`)
	return err
}

func (s *Store) Status(ctx context.Context) (Status, error) {
	status := Status{DBPath: s.path}
	counts := []struct {
		query string
		dest  *int
	}{
		{`select count(*) from spaces`, &status.Spaces},
		{`select count(*) from users`, &status.Users},
		{`select count(*) from teams`, &status.Teams},
		{`select count(*) from pages`, &status.Pages},
		{`select count(*) from blocks`, &status.Blocks},
		{`select count(*) from collections`, &status.Collections},
		{`select count(*) from comments`, &status.Comments},
		{`select count(*) from raw_records`, &status.RawRecords},
	}
	for _, count := range counts {
		if err := s.db.QueryRowContext(ctx, count.query).Scan(count.dest); err != nil {
			return Status{}, err
		}
	}
	if err := s.db.QueryRowContext(ctx, `select coalesce(max(synced_at), 0) from sync_state`).Scan(&status.LastSyncAt); err != nil {
		return Status{}, err
	}
	status.DBBytes = fileSize(s.path)
	status.WALBytes = fileSize(s.path + "-wal")
	return status, nil
}

func (s *Store) Optimize(ctx context.Context, vacuum bool) (MaintenanceSummary, error) {
	if err := s.RebuildFTS(ctx); err != nil {
		return MaintenanceSummary{}, err
	}
	for _, stmt := range []string{
		`insert into page_fts(page_fts) values('optimize')`,
		`insert into comment_fts(comment_fts) values('optimize')`,
		`pragma optimize`,
		`analyze`,
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return MaintenanceSummary{}, err
		}
	}
	if vacuum {
		if _, err := s.db.ExecContext(ctx, `vacuum`); err != nil {
			return MaintenanceSummary{}, err
		}
	}
	return MaintenanceSummary{
		RebuiltFTS: true,
		Optimized:  true,
		Analyzed:   true,
		Vacuumed:   vacuum,
		DBBytes:    fileSize(s.path),
		WALBytes:   fileSize(s.path + "-wal"),
		Message:    "database maintenance complete",
	}, nil
}

func fileSize(path string) int64 {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return 0
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
