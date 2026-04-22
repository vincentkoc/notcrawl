package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 1

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	st := &Store{db: db}
	if err := st.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) init(ctx context.Context) error {
	stmts := []string{
		`pragma foreign_keys = on`,
		`pragma journal_mode = wal`,
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
			name text,
			schema_json text,
			format_json text,
			raw_json text,
			source text not null,
			synced_at integer not null
		)`,
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
		`create table if not exists sync_state (
			source text not null,
			entity_type text not null,
			entity_id text not null,
			cursor text,
			synced_at integer not null,
			primary key (source, entity_type, entity_id)
		)`,
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
		return fmt.Errorf("database schema version %d is newer than this notioncrawl build supports (%d)", current, schemaVersion)
	}
	if _, err := s.db.ExecContext(ctx, `insert or replace into meta(key, value) values('schema_version', ?)`, schemaVersion); err != nil {
		return err
	}
	return nil
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
	return s.refreshPageFTS(ctx, x.ID)
}

func (s *Store) UpsertBlock(ctx context.Context, x Block) error {
	_, err := s.db.ExecContext(ctx, `insert into blocks(
		id, page_id, space_id, parent_id, parent_table, type, text, properties_json, content_json, format_json,
		created_time, last_edited_time, alive, source, raw_json, synced_at)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
			created_time=excluded.created_time,
			last_edited_time=excluded.last_edited_time,
			alive=excluded.alive,
			source=excluded.source,
			raw_json=excluded.raw_json,
			synced_at=excluded.synced_at`,
		x.ID, x.PageID, x.SpaceID, x.ParentID, x.ParentTable, x.Type, x.Text, x.PropertiesJSON, x.ContentJSON, x.FormatJSON,
		x.CreatedTime, x.LastEditedTime, BoolInt(x.Alive), x.Source, x.RawJSON, x.SyncedAt)
	if err != nil {
		return err
	}
	if x.PageID != "" {
		return s.refreshPageFTS(ctx, x.PageID)
	}
	return nil
}

func (s *Store) UpsertCollection(ctx context.Context, x Collection) error {
	_, err := s.db.ExecContext(ctx, `insert into collections(id, space_id, parent_id, name, schema_json, format_json, raw_json, source, synced_at)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(id) do update set space_id=excluded.space_id, parent_id=excluded.parent_id, name=excluded.name,
			schema_json=excluded.schema_json, format_json=excluded.format_json, raw_json=excluded.raw_json,
			source=excluded.source, synced_at=excluded.synced_at`,
		x.ID, x.SpaceID, x.ParentID, x.Name, x.SchemaJSON, x.FormatJSON, x.RawJSON, x.Source, x.SyncedAt)
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

func (s *Store) refreshPageFTS(ctx context.Context, pageID string) error {
	var title string
	if err := s.db.QueryRowContext(ctx, `select title from pages where id = ?`, pageID).Scan(&title); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	rows, err := s.db.QueryContext(ctx, `select text from blocks where page_id = ? and alive = 1 order by created_time, id`, pageID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var text sql.NullString
		if err := rows.Scan(&text); err != nil {
			return err
		}
		if text.Valid && strings.TrimSpace(text.String) != "" {
			parts = append(parts, text.String)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `delete from page_fts where page_id = ?`, pageID); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `insert into page_fts(page_id, title, body) values (?, ?, ?)`, pageID, title, strings.Join(parts, "\n"))
	return err
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
