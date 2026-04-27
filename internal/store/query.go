package store

import (
	"context"
	"database/sql"
	"strings"
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
	rows, err := s.db.QueryContext(ctx, `select id, space_id, parent_id, parent_table, name, schema_json, format_json, raw_json, source, synced_at
		from collections order by lower(coalesce(name, id)), id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var collections []Collection
	for rows.Next() {
		var c Collection
		if err := rows.Scan(&c.ID, &c.SpaceID, &c.ParentID, &c.ParentTable, &c.Name, &c.SchemaJSON, &c.FormatJSON, &c.RawJSON, &c.Source, &c.SyncedAt); err != nil {
			return nil, err
		}
		collections = append(collections, c)
	}
	return collections, rows.Err()
}

func (s *Store) Collection(ctx context.Context, id string) (Collection, error) {
	var c Collection
	err := s.db.QueryRowContext(ctx, `select id, space_id, parent_id, parent_table, name, schema_json, format_json, raw_json, source, synced_at
		from collections where id = ?`, id).Scan(&c.ID, &c.SpaceID, &c.ParentID, &c.ParentTable, &c.Name, &c.SchemaJSON, &c.FormatJSON, &c.RawJSON, &c.Source, &c.SyncedAt)
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
		content_json, format_json, display_order, created_time, last_edited_time, alive, source, raw_json, synced_at
		from blocks where page_id = ? and alive = 1 order by parent_id, display_order, created_time, id`, pageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var blocks []Block
	for rows.Next() {
		var b Block
		var alive int
		if err := rows.Scan(&b.ID, &b.PageID, &b.SpaceID, &b.ParentID, &b.ParentTable, &b.Type, &b.Text, &b.PropertiesJSON,
			&b.ContentJSON, &b.FormatJSON, &b.DisplayOrder, &b.CreatedTime, &b.LastEditedTime, &alive, &b.Source, &b.RawJSON, &b.SyncedAt); err != nil {
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
			return "space-" + shortID(id), nil
		}
		return "", err
	}
	if name.Valid && name.String != "" {
		return name.String, nil
	}
	return "space-" + shortID(id), nil
}

func (s *Store) TeamName(ctx context.Context, id string) (string, error) {
	if id == "" {
		return "", nil
	}
	var name sql.NullString
	err := s.db.QueryRowContext(ctx, `select name from teams where id = ?`, id).Scan(&name)
	if err != nil {
		if err == sql.ErrNoRows {
			return "team-" + shortID(id), nil
		}
		return "", err
	}
	if name.Valid && name.String != "" {
		return name.String, nil
	}
	return "team-" + shortID(id), nil
}

func (s *Store) PageTeamID(ctx context.Context, page Page) (string, error) {
	seen := map[string]bool{page.ID: true}
	return s.resolveTeamID(ctx, page.ParentTable, page.ParentID, page.CollectionID, seen)
}

func (s *Store) resolveTeamID(ctx context.Context, table, id, collectionID string, seen map[string]bool) (string, error) {
	if table == "team" {
		return id, nil
	}
	if table == "collection" && id == "" {
		id = collectionID
	}
	if id == "" || seen[table+":"+id] {
		return "", nil
	}
	seen[table+":"+id] = true
	switch table {
	case "block":
		var parentID, parentTable sql.NullString
		err := s.db.QueryRowContext(ctx, `select parent_id, parent_table from blocks where id = ?`, id).Scan(&parentID, &parentTable)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", nil
			}
			return "", err
		}
		return s.resolveTeamID(ctx, parentTable.String, parentID.String, "", seen)
	case "collection", "database", "data_source":
		var parentID, parentTable sql.NullString
		err := s.db.QueryRowContext(ctx, `select parent_id, parent_table from collections where id = ?`, id).Scan(&parentID, &parentTable)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", nil
			}
			return "", err
		}
		return s.resolveTeamID(ctx, parentTable.String, parentID.String, "", seen)
	default:
		return "", nil
	}
}

func shortID(id string) string {
	clean := strings.ReplaceAll(id, "-", "")
	if len(clean) > 16 {
		return clean[:8] + "-" + clean[len(clean)-8:]
	}
	if clean == "" {
		return "unknown"
	}
	return clean
}
