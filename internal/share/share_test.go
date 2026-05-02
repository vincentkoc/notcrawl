package share

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vincentkoc/notcrawl/internal/markdown"
	"github.com/vincentkoc/notcrawl/internal/store"
)

func TestPublishAndImportSnapshot(t *testing.T) {
	ctx := context.Background()
	src, err := store.Open(filepath.Join(t.TempDir(), "src.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	now := store.NowMS()
	if err := src.UpsertPage(ctx, store.Page{ID: "page1", Title: "Launch", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := src.UpsertBlock(ctx, store.Block{ID: "block1", PageID: "page1", ParentID: "page1", Type: "text", Text: "hello", Alive: true, Source: "test", SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	mdDir := t.TempDir()
	if _, err := (markdown.Exporter{Store: src, Dir: mdDir}).Export(ctx); err != nil {
		t.Fatal(err)
	}
	repo := t.TempDir()
	s, err := Publish(ctx, src, PublishOptions{RepoPath: repo, MarkdownDir: mdDir})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Manifest.Tables) == 0 {
		t.Fatal("expected tables in manifest")
	}
	if _, err := os.Stat(filepath.Join(repo, "pages", "default", "launch-page1.md")); err != nil {
		t.Fatal(err)
	}
	stalePage := filepath.Join(repo, "pages", "default", "stale.md")
	if err := os.WriteFile(stalePage, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	pageSidecar := filepath.Join(repo, "pages", "default", "README.txt")
	if err := os.WriteFile(pageSidecar, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	staleData := filepath.Join(repo, "data", "stale.jsonl.gz")
	if err := os.WriteFile(staleData, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	dataSidecar := filepath.Join(repo, "data", "README.txt")
	if err := os.WriteFile(dataSidecar, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, src, PublishOptions{RepoPath: repo, MarkdownDir: mdDir}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{stalePage, staleData} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected generated stale file %s to be pruned, got %v", path, err)
		}
	}
	for _, path := range []string{pageSidecar, dataSidecar} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected sidecar %s to remain: %v", path, err)
		}
	}
	dst, err := store.Open(filepath.Join(t.TempDir(), "dst.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if _, err := Import(ctx, dst, repo); err != nil {
		t.Fatal(err)
	}
	results, err := dst.Search(ctx, "hello", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected imported search result, got %d", len(results))
	}
}

func TestEnsureRepoUpdatesExistingOrigin(t *testing.T) {
	ctx := context.Background()
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	runGitForTest(t, repo, "init")
	runGitForTest(t, repo, "remote", "add", "origin", "https://example.invalid/old.git")

	const remote = "https://example.invalid/fresh.git"
	if err := ensureRepo(ctx, repo, remote, "main"); err != nil {
		t.Fatal(err)
	}

	got := gitOutputForTest(t, repo, "remote", "get-url", "origin")
	if strings.TrimSpace(got) != remote {
		t.Fatalf("origin = %q", got)
	}
}

func runGitForTest(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
}

func gitOutputForTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out)
}
