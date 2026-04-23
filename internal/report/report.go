package report

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/vincentkoc/notcrawl/internal/store"
)

type Options struct {
	Now time.Time
}

type ActivityReport struct {
	GeneratedAt    time.Time      `json:"generated_at"`
	LatestEditedAt *time.Time     `json:"latest_edited_at,omitempty"`
	TotalSpaces    int            `json:"total_spaces"`
	TotalUsers     int            `json:"total_users"`
	TotalPages     int            `json:"total_pages"`
	TotalBlocks    int            `json:"total_blocks"`
	TotalDatabases int            `json:"total_databases"`
	TotalComments  int            `json:"total_comments"`
	TotalRaw       int            `json:"total_raw_records"`
	Windows        []WindowStats  `json:"windows"`
	TopCollections []RankedCount  `json:"top_collections"`
	TopSpaces      []RankedCount  `json:"top_spaces"`
	RecentPages    []PageActivity `json:"recent_pages"`
}

type WindowStats struct {
	Label             string    `json:"label"`
	Since             time.Time `json:"since"`
	EditedPages       int       `json:"edited_pages"`
	ActiveCollections int       `json:"active_collections"`
	Comments          int       `json:"comments"`
}

type RankedCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type PageActivity struct {
	ID           string     `json:"id"`
	Title        string     `json:"title"`
	CollectionID string     `json:"collection_id,omitempty"`
	SpaceID      string     `json:"space_id,omitempty"`
	EditedAt     *time.Time `json:"edited_at,omitempty"`
}

func Build(ctx context.Context, st *store.Store, opts Options) (ActivityReport, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	report := ActivityReport{GeneratedAt: now.UTC()}
	if err := scanTotals(ctx, st.DB(), &report); err != nil {
		return ActivityReport{}, err
	}
	var anchor time.Time
	if report.LatestEditedAt != nil {
		anchor = *report.LatestEditedAt
	}
	if anchor.IsZero() {
		anchor = now.UTC()
	}
	for _, window := range []struct {
		label string
		dur   time.Duration
	}{
		{"24 hours", 24 * time.Hour},
		{"7 days", 7 * 24 * time.Hour},
		{"30 days", 30 * 24 * time.Hour},
	} {
		stats, err := scanWindow(ctx, st.DB(), window.label, anchor.Add(-window.dur))
		if err != nil {
			return ActivityReport{}, err
		}
		report.Windows = append(report.Windows, stats)
	}
	weekSince := anchor.Add(-7 * 24 * time.Hour)
	var err error
	report.TopCollections, err = ranked(ctx, st.DB(), `
select coalesce(nullif(c.name, ''), nullif(p.collection_id, ''), 'no database') as name, count(*) as total
from pages p
left join collections c on c.id = p.collection_id
where p.alive = 1 and coalesce(p.last_edited_time, p.created_time, 0) >= ?
group by coalesce(nullif(c.name, ''), nullif(p.collection_id, ''), 'no database')
order by total desc, name asc
limit ?
`, unixMilli(weekSince), 8)
	if err != nil {
		return ActivityReport{}, err
	}
	if report.TopCollections == nil {
		report.TopCollections = []RankedCount{}
	}
	report.TopSpaces, err = ranked(ctx, st.DB(), `
select coalesce(nullif(s.name, ''), nullif(p.space_id, ''), 'default') as name, count(*) as total
from pages p
left join spaces s on s.id = p.space_id
where p.alive = 1 and coalesce(p.last_edited_time, p.created_time, 0) >= ?
group by coalesce(nullif(s.name, ''), nullif(p.space_id, ''), 'default')
order by total desc, name asc
limit ?
`, unixMilli(weekSince), 8)
	if err != nil {
		return ActivityReport{}, err
	}
	if report.TopSpaces == nil {
		report.TopSpaces = []RankedCount{}
	}
	report.RecentPages, err = recentPages(ctx, st.DB(), 8)
	if err != nil {
		return ActivityReport{}, err
	}
	if report.RecentPages == nil {
		report.RecentPages = []PageActivity{}
	}
	return report, nil
}

func scanTotals(ctx context.Context, db *sql.DB, report *ActivityReport) error {
	var latest sql.NullInt64
	if err := db.QueryRowContext(ctx, `
select
	(select count(*) from spaces),
	(select count(*) from users),
	(select count(*) from pages where alive = 1),
	(select count(*) from blocks where alive = 1),
	(select count(*) from collections),
	(select count(*) from comments where alive = 1),
	(select count(*) from raw_records),
	(select max(coalesce(last_edited_time, created_time, 0)) from pages where alive = 1)
`).Scan(
		&report.TotalSpaces,
		&report.TotalUsers,
		&report.TotalPages,
		&report.TotalBlocks,
		&report.TotalDatabases,
		&report.TotalComments,
		&report.TotalRaw,
		&latest,
	); err != nil {
		return fmt.Errorf("scan report totals: %w", err)
	}
	if latest.Valid && latest.Int64 > 0 {
		t := time.UnixMilli(latest.Int64).UTC()
		report.LatestEditedAt = &t
	}
	return nil
}

func scanWindow(ctx context.Context, db *sql.DB, label string, since time.Time) (WindowStats, error) {
	stats := WindowStats{Label: label, Since: since.UTC()}
	cutoff := unixMilli(since)
	if err := db.QueryRowContext(ctx, `
select
	(select count(*) from pages where alive = 1 and coalesce(last_edited_time, created_time, 0) >= ?),
	(select count(distinct nullif(collection_id, '')) from pages where alive = 1 and coalesce(last_edited_time, created_time, 0) >= ?),
	(select count(*) from comments where alive = 1 and coalesce(created_time, 0) >= ?)
`, cutoff, cutoff, cutoff).Scan(&stats.EditedPages, &stats.ActiveCollections, &stats.Comments); err != nil {
		return WindowStats{}, fmt.Errorf("scan %s stats: %w", label, err)
	}
	return stats, nil
}

func ranked(ctx context.Context, db *sql.DB, query string, args ...any) ([]RankedCount, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RankedCount
	for rows.Next() {
		var row RankedCount
		if err := rows.Scan(&row.Name, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func recentPages(ctx context.Context, db *sql.DB, limit int) ([]PageActivity, error) {
	rows, err := db.QueryContext(ctx, `
select id, title, coalesce(collection_id, ''), coalesce(space_id, ''), coalesce(last_edited_time, created_time, 0)
from pages
where alive = 1
order by coalesce(last_edited_time, created_time, 0) desc, title asc
limit ?
`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PageActivity
	for rows.Next() {
		var row PageActivity
		var edited int64
		if err := rows.Scan(&row.ID, &row.Title, &row.CollectionID, &row.SpaceID, &edited); err != nil {
			return nil, err
		}
		if edited > 0 {
			t := time.UnixMilli(edited).UTC()
			row.EditedAt = &t
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func unixMilli(t time.Time) int64 {
	return t.UTC().UnixMilli()
}
