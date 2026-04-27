package share

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vincentkoc/notcrawl/internal/store"
)

var exportTables = []string{
	"spaces",
	"users",
	"teams",
	"pages",
	"blocks",
	"collections",
	"comments",
	"raw_records",
	"sync_state",
}

type Manifest struct {
	GeneratedAt string          `json:"generated_at"`
	Tables      []TableManifest `json:"tables"`
}

type TableManifest struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Rows int    `json:"rows"`
}

type PublishOptions struct {
	RepoPath    string
	Remote      string
	Branch      string
	MarkdownDir string
	Message     string
	Push        bool
	Commit      bool
}

type PublishSummary struct {
	Manifest  Manifest
	Committed bool
	Pushed    bool
}

func Publish(ctx context.Context, st *store.Store, opts PublishOptions) (PublishSummary, error) {
	if opts.RepoPath == "" {
		return PublishSummary{}, fmt.Errorf("missing share repo path")
	}
	if opts.Branch == "" {
		opts.Branch = "main"
	}
	if opts.Message == "" {
		opts.Message = "archive: notcrawl snapshot"
	}
	if err := ensureRepo(ctx, opts.RepoPath, opts.Remote, opts.Branch); err != nil {
		return PublishSummary{}, err
	}
	if err := os.RemoveAll(filepath.Join(opts.RepoPath, "data")); err != nil {
		return PublishSummary{}, err
	}
	if err := os.RemoveAll(filepath.Join(opts.RepoPath, "pages")); err != nil {
		return PublishSummary{}, err
	}
	if err := os.MkdirAll(filepath.Join(opts.RepoPath, "data"), 0o755); err != nil {
		return PublishSummary{}, err
	}
	manifest := Manifest{GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	for _, table := range exportTables {
		tm, err := exportTable(ctx, st.DB(), opts.RepoPath, table)
		if err != nil {
			return PublishSummary{}, err
		}
		manifest.Tables = append(manifest.Tables, tm)
	}
	if opts.MarkdownDir != "" {
		if err := copyDir(opts.MarkdownDir, filepath.Join(opts.RepoPath, "pages")); err != nil && !os.IsNotExist(err) {
			return PublishSummary{}, err
		}
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return PublishSummary{}, err
	}
	if err := os.WriteFile(filepath.Join(opts.RepoPath, "manifest.json"), append(b, '\n'), 0o644); err != nil {
		return PublishSummary{}, err
	}
	s := PublishSummary{Manifest: manifest}
	if opts.Commit {
		if err := runGit(ctx, opts.RepoPath, "add", "manifest.json", "data", "pages"); err != nil {
			return s, err
		}
		dirty, err := hasChanges(ctx, opts.RepoPath)
		if err != nil {
			return s, err
		}
		if dirty {
			if err := runGit(ctx, opts.RepoPath, "commit", "-m", opts.Message); err != nil {
				return s, err
			}
			s.Committed = true
		}
	}
	if opts.Push {
		if err := runGit(ctx, opts.RepoPath, "push", "-u", "origin", opts.Branch); err != nil {
			return s, err
		}
		s.Pushed = true
	}
	return s, nil
}

func Import(ctx context.Context, st *store.Store, repoPath string) (Manifest, error) {
	b, err := os.ReadFile(filepath.Join(repoPath, "manifest.json"))
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(b, &manifest); err != nil {
		return Manifest{}, err
	}
	for _, table := range manifest.Tables {
		if err := importTable(ctx, st.DB(), filepath.Join(repoPath, table.Path), table.Name); err != nil {
			return manifest, err
		}
	}
	if err := st.RebuildFTS(ctx); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func Subscribe(ctx context.Context, st *store.Store, remote, repoPath, branch string) (Manifest, error) {
	if remote == "" {
		return Manifest{}, fmt.Errorf("missing share remote")
	}
	if branch == "" {
		branch = "main"
	}
	if _, err := os.Stat(filepath.Join(repoPath, ".git")); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(repoPath), 0o755); err != nil {
			return Manifest{}, err
		}
		if err := run(ctx, "", "git", "clone", "--branch", branch, remote, repoPath); err != nil {
			return Manifest{}, err
		}
	} else if err == nil {
		if err := runGit(ctx, repoPath, "pull", "--ff-only", "origin", branch); err != nil {
			return Manifest{}, err
		}
	} else {
		return Manifest{}, err
	}
	return Import(ctx, st, repoPath)
}

func Update(ctx context.Context, st *store.Store, repoPath, branch string) (Manifest, error) {
	if branch == "" {
		branch = "main"
	}
	if err := runGit(ctx, repoPath, "pull", "--ff-only", "origin", branch); err != nil {
		return Manifest{}, err
	}
	return Import(ctx, st, repoPath)
}

func exportTable(ctx context.Context, db *sql.DB, repoPath, table string) (TableManifest, error) {
	path := filepath.Join("data", table+".jsonl.gz")
	full := filepath.Join(repoPath, path)
	out, err := os.Create(full)
	if err != nil {
		return TableManifest{}, err
	}
	defer out.Close()
	gz := gzip.NewWriter(out)
	defer gz.Close()
	rows, err := db.QueryContext(ctx, "select * from "+quoteIdent(table))
	if err != nil {
		return TableManifest{}, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return TableManifest{}, err
	}
	count := 0
	enc := json.NewEncoder(gz)
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return TableManifest{}, err
		}
		row := map[string]any{}
		for i, col := range cols {
			row[col] = exportValue(values[i])
		}
		if err := enc.Encode(row); err != nil {
			return TableManifest{}, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return TableManifest{}, err
	}
	return TableManifest{Name: table, Path: path, Rows: count}, nil
}

func importTable(ctx context.Context, db *sql.DB, path, table string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	gz, err := gzip.NewReader(in)
	if err != nil {
		return err
	}
	defer gz.Close()
	if _, err := db.ExecContext(ctx, "delete from "+quoteIdent(table)); err != nil {
		return err
	}
	scanner := bufio.NewScanner(gz)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 32*1024*1024)
	for scanner.Scan() {
		var row map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return err
		}
		if len(row) == 0 {
			continue
		}
		cols := make([]string, 0, len(row))
		for col := range row {
			cols = append(cols, col)
		}
		sort.Strings(cols)
		args := make([]any, 0, len(cols))
		holders := make([]string, 0, len(cols))
		quotedCols := make([]string, 0, len(cols))
		for _, col := range cols {
			quotedCols = append(quotedCols, quoteIdent(col))
			holders = append(holders, "?")
			args = append(args, row[col])
		}
		stmt := fmt.Sprintf("insert or replace into %s(%s) values(%s)", quoteIdent(table), strings.Join(quotedCols, ","), strings.Join(holders, ","))
		if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func ensureRepo(ctx context.Context, repoPath, remote, branch string) error {
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(repoPath, ".git")); os.IsNotExist(err) {
		if err := runGit(ctx, repoPath, "init", "-b", branch); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if remote != "" {
		if err := runGit(ctx, repoPath, "remote", "get-url", "origin"); err != nil {
			if err := runGit(ctx, repoPath, "remote", "add", "origin", remote); err != nil {
				return err
			}
		} else if err := runGit(ctx, repoPath, "remote", "set-url", "origin", remote); err != nil {
			return err
		}
	}
	return nil
}

func hasChanges(ctx context.Context, repoPath string) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "status", "--porcelain")
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) != "", nil
}

func runGit(ctx context.Context, dir string, args ...string) error {
	return run(ctx, dir, "git", append([]string{"-C", dir}, args...)...)
}

func run(ctx context.Context, dir, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func copyDir(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("not a directory: %s", src)
	}
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return os.MkdirAll(dst, 0o755)
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
}

func exportValue(v any) any {
	switch x := v.(type) {
	case []byte:
		return string(x)
	default:
		return x
	}
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
