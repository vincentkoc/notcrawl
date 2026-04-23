package store

import (
	"context"
	"database/sql"
)

func (s *Store) Pages(ctx context.Context) ([]Page, error) {
	rows, err := s.db.QueryContext(ctx, `select id, space_id, parent_id, parent_table, collection_id, title, url, icon, cover,
		properties_json, created_time, last_edited_time, alive, source, raw_json, synced_at
		from pages where alive = 1 order by coalesce(last_edited_time, 0) desc, title`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pages []Page
	for rows.Next() {
		var p Page
		var alive int
		if err := rows.Scan(&p.ID, &p.SpaceID, &p.ParentID, &p.ParentTable, &p.CollectionID, &p.Title, &p.URL, &p.Icon, &p.Cover,
			&p.PropertiesJSON, &p.CreatedTime, &p.LastEditedTime, &alive, &p.Source, &p.RawJSON, &p.SyncedAt); err != nil {
			return nil, err
		}
		p.Alive = IntBool(alive)
		pages = append(pages, p)
	}
	return pages, rows.Err()
}

func (s *Store) Collections(ctx context.Context) ([]Collection, error) {
	rows, err := s.db.QueryContext(ctx, `select id, space_id, parent_id, name, schema_json, format_json, raw_json, source, synced_at
		from collections order by lower(coalesce(name, id)), id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var collections []Collection
	for rows.Next() {
		var c Collection
		if err := rows.Scan(&c.ID, &c.SpaceID, &c.ParentID, &c.Name, &c.SchemaJSON, &c.FormatJSON, &c.RawJSON, &c.Source, &c.SyncedAt); err != nil {
			return nil, err
		}
		collections = append(collections, c)
	}
	return collections, rows.Err()
}

func (s *Store) Collection(ctx context.Context, id string) (Collection, error) {
	var c Collection
	err := s.db.QueryRowContext(ctx, `select id, space_id, parent_id, name, schema_json, format_json, raw_json, source, synced_at
		from collections where id = ?`, id).Scan(&c.ID, &c.SpaceID, &c.ParentID, &c.Name, &c.SchemaJSON, &c.FormatJSON, &c.RawJSON, &c.Source, &c.SyncedAt)
	return c, err
}

func (s *Store) CollectionPages(ctx context.Context, collectionID string) ([]Page, error) {
	rows, err := s.db.QueryContext(ctx, `select id, space_id, parent_id, parent_table, collection_id, title, url, icon, cover,
		properties_json, created_time, last_edited_time, alive, source, raw_json, synced_at
		from pages where collection_id = ? and alive = 1 order by coalesce(last_edited_time, 0) desc, title`, collectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pages []Page
	for rows.Next() {
		var p Page
		var alive int
		if err := rows.Scan(&p.ID, &p.SpaceID, &p.ParentID, &p.ParentTable, &p.CollectionID, &p.Title, &p.URL, &p.Icon, &p.Cover,
			&p.PropertiesJSON, &p.CreatedTime, &p.LastEditedTime, &alive, &p.Source, &p.RawJSON, &p.SyncedAt); err != nil {
			return nil, err
		}
		p.Alive = IntBool(alive)
		pages = append(pages, p)
	}
	return pages, rows.Err()
}

func (s *Store) PageBlocks(ctx context.Context, pageID string) ([]Block, error) {
	rows, err := s.db.QueryContext(ctx, `select id, page_id, space_id, parent_id, parent_table, type, text, properties_json,
		content_json, format_json, created_time, last_edited_time, alive, source, raw_json, synced_at
		from blocks where page_id = ? and alive = 1 order by created_time, id`, pageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var blocks []Block
	for rows.Next() {
		var b Block
		var alive int
		if err := rows.Scan(&b.ID, &b.PageID, &b.SpaceID, &b.ParentID, &b.ParentTable, &b.Type, &b.Text, &b.PropertiesJSON,
			&b.ContentJSON, &b.FormatJSON, &b.CreatedTime, &b.LastEditedTime, &alive, &b.Source, &b.RawJSON, &b.SyncedAt); err != nil {
			return nil, err
		}
		b.Alive = IntBool(alive)
		blocks = append(blocks, b)
	}
	return blocks, rows.Err()
}

func (s *Store) PageComments(ctx context.Context, pageID string) ([]Comment, error) {
	rows, err := s.db.QueryContext(ctx, `select id, page_id, space_id, parent_id, text, created_by_id,
		created_time, last_edited_time, alive, raw_json, source, synced_at
		from comments where page_id = ? and alive = 1 order by created_time, id`, pageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var comments []Comment
	for rows.Next() {
		var c Comment
		var alive int
		if err := rows.Scan(&c.ID, &c.PageID, &c.SpaceID, &c.ParentID, &c.Text, &c.CreatedByID,
			&c.CreatedTime, &c.LastEditedTime, &alive, &c.RawJSON, &c.Source, &c.SyncedAt); err != nil {
			return nil, err
		}
		c.Alive = IntBool(alive)
		comments = append(comments, c)
	}
	return comments, rows.Err()
}

func (s *Store) SpaceName(ctx context.Context, id string) (string, error) {
	if id == "" {
		return "default", nil
	}
	var name sql.NullString
	err := s.db.QueryRowContext(ctx, `select name from spaces where id = ?`, id).Scan(&name)
	if err != nil {
		if err == sql.ErrNoRows {
			return id, nil
		}
		return "", err
	}
	if name.Valid && name.String != "" {
		return name.String, nil
	}
	return id, nil
}
