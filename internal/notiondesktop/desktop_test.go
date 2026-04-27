package notiondesktop

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
