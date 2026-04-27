package notiondesktop

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vincentkoc/notcrawl/internal/store"
	_ "modernc.org/sqlite"
)

func TestPruneDesktopSnapshotsKeepsNewestAndSidecars(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"notion-desktop-1000.db",
		"notion-desktop-2000.db",
		"notion-desktop-3000.db",
	}
	for i, name := range names {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, suffix := range []string{"-wal", "-shm"} {
			if err := os.WriteFile(path+suffix, []byte(suffix), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		modTime := time.Unix(int64(i+1), 0)
		for _, target := range []string{path, path + "-wal", path + "-shm"} {
			if err := os.Chtimes(target, modTime, modTime); err != nil {
				t.Fatal(err)
			}
		}
	}

	current := filepath.Join(dir, "notion-desktop-3000.db")
	if err := pruneDesktopSnapshots(dir, 2, current); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"notion-desktop-2000.db", "notion-desktop-3000.db"} {
		path := filepath.Join(dir, name)
		for _, target := range []string{path, path + "-wal", path + "-shm"} {
			if _, err := os.Stat(target); err != nil {
				t.Fatalf("expected %s to remain: %v", target, err)
			}
		}
	}
	for _, target := range []string{
		filepath.Join(dir, "notion-desktop-1000.db"),
		filepath.Join(dir, "notion-desktop-1000.db-wal"),
		filepath.Join(dir, "notion-desktop-1000.db-shm"),
	} {
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be pruned, got %v", target, err)
		}
	}
}

func TestIngestBlocksDerivesUntitledPageFromChildText(t *testing.T) {
	ctx := context.Background()
	src, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "desktop.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.ExecContext(ctx, `create table block (
		id text primary key,
		space_id text,
		type text,
		properties text,
		content text,
		collection_id text,
		created_time integer,
		last_edited_time integer,
		parent_id text,
		parent_table text,
		alive integer,
		format text
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := src.ExecContext(ctx, `insert into block(id, space_id, type, properties, content, collection_id, created_time, last_edited_time, parent_id, parent_table, alive, format)
		values
		('page1', 'space1', 'page', '{}', '', '', 1, 1, '', '', 1, ''),
		('child1', 'space1', 'text', '{"title":[["Decision log"]]}', '', '', 2, 2, 'page1', 'block', 1, '')`); err != nil {
		t.Fatal(err)
	}

	st, err := store.Open(filepath.Join(t.TempDir(), "notcrawl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, _, _, err := ingestBlocks(ctx, st, src); err != nil {
		t.Fatal(err)
	}

	var title string
	if err := st.DB().QueryRowContext(ctx, `select title from pages where id = 'page1'`).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if title != "Decision log" {
		t.Fatalf("expected child text title, got %q", title)
	}
}
