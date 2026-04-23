package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/vincentkoc/notcrawl/internal/config"
	"github.com/vincentkoc/notcrawl/internal/markdown"
	"github.com/vincentkoc/notcrawl/internal/notionapi"
	"github.com/vincentkoc/notcrawl/internal/notiondesktop"
	"github.com/vincentkoc/notcrawl/internal/share"
	"github.com/vincentkoc/notcrawl/internal/store"
	"github.com/vincentkoc/notcrawl/internal/tableexport"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "notcrawl:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		printHelp(stdout)
		return nil
	}
	global := flag.NewFlagSet("notcrawl", flag.ContinueOnError)
	global.SetOutput(stderr)
	configPath := global.String("config", "", "config file path")
	dbPath := global.String("db", "", "database path override")
	if err := global.Parse(args); err != nil {
		return err
	}
	rest := global.Args()
	if len(rest) == 0 || rest[0] == "help" || rest[0] == "--help" || rest[0] == "-h" {
		printHelp(stdout)
		return nil
	}
	cmd := rest[0]
	cmdArgs := rest[1:]
	if cmd == "init" {
		path, err := config.WriteStarter(*configPath)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "wrote %s\n", path)
		return nil
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *dbPath != "" {
		cfg.DBPath, err = config.ExpandPath(*dbPath)
		if err != nil {
			return err
		}
	}
	switch cmd {
	case "doctor":
		return runDoctor(ctx, stdout, cfg)
	case "sync":
		return runSync(ctx, stdout, cfg, cmdArgs)
	case "export-md":
		return runExportMarkdown(ctx, stdout, cfg)
	case "databases":
		return runDatabases(ctx, stdout, cfg)
	case "export-db":
		return runExportDatabase(ctx, stdout, cfg, cmdArgs)
	case "search":
		return runSearch(ctx, stdout, cfg, cmdArgs)
	case "sql":
		return runSQL(ctx, stdout, cfg, cmdArgs)
	case "publish":
		return runPublish(ctx, stdout, cfg, cmdArgs)
	case "subscribe":
		return runSubscribe(ctx, stdout, cfg, cmdArgs)
	case "update":
		return runUpdate(ctx, stdout, cfg, cmdArgs)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func runDoctor(ctx context.Context, stdout io.Writer, cfg config.Config) error {
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	desktop, err := notiondesktop.Inspect(cfg.Notion.Desktop.Path)
	if err != nil {
		return err
	}
	report := map[string]any{
		"db_path":           cfg.DBPath,
		"cache_dir":         cfg.CacheDir,
		"markdown_dir":      cfg.MarkdownDir,
		"desktop_path":      desktop.Path,
		"desktop_available": desktop.Available,
		"desktop_size":      desktop.SizeBytes,
		"api_token_env":     cfg.Notion.API.TokenEnv,
		"api_token_present": cfg.APIToken() != "",
	}
	_ = ctx
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, string(b))
	return nil
}

func runSync(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	source := fs.String("source", "all", "source: desktop, api, all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	switch *source {
	case "desktop":
		s, err := notiondesktop.Ingest(ctx, st, cfg.Notion.Desktop.Path, cfg.CacheDir)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "desktop: pages=%d blocks=%d collections=%d comments=%d snapshot=%s\n", s.Pages, s.Blocks, s.Collections, s.Comments, s.Source.Snapshot)
	case "api":
		s, err := notionapi.Client{
			BaseURL: cfg.Notion.API.BaseURL,
			Version: cfg.Notion.API.Version,
			Token:   cfg.APIToken(),
		}.Sync(ctx, st)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "api: users=%d pages=%d databases=%d database_rows=%d blocks=%d comments=%d\n", s.Users, s.Pages, s.Databases, s.DatabaseRows, s.Blocks, s.Comments)
	case "all":
		if cfg.Notion.Desktop.Enabled {
			s, err := notiondesktop.Ingest(ctx, st, cfg.Notion.Desktop.Path, cfg.CacheDir)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "desktop: pages=%d blocks=%d collections=%d comments=%d snapshot=%s\n", s.Pages, s.Blocks, s.Collections, s.Comments, s.Source.Snapshot)
		}
		if cfg.Notion.API.Enabled && cfg.APIToken() != "" {
			s, err := notionapi.Client{
				BaseURL: cfg.Notion.API.BaseURL,
				Version: cfg.Notion.API.Version,
				Token:   cfg.APIToken(),
			}.Sync(ctx, st)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, "api: users=%d pages=%d databases=%d database_rows=%d blocks=%d comments=%d\n", s.Users, s.Pages, s.Databases, s.DatabaseRows, s.Blocks, s.Comments)
		}
	default:
		return fmt.Errorf("unknown source %q", *source)
	}
	return nil
}

func runExportMarkdown(ctx context.Context, stdout io.Writer, cfg config.Config) error {
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	s, err := markdown.Exporter{Store: st, Dir: cfg.MarkdownDir}.Export(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "exported %d pages to %s\n", s.Pages, cfg.MarkdownDir)
	return nil
}

func runDatabases(ctx context.Context, stdout io.Writer, cfg config.Config) error {
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	collections, err := st.Collections(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "id\tname\tsource")
	for _, collection := range collections {
		fmt.Fprintf(stdout, "%s\t%s\t%s\n", collection.ID, collection.Name, collection.Source)
	}
	return nil
}

func runExportDatabase(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("export-db", flag.ContinueOnError)
	databaseID := fs.String("database", "", "database id to export")
	format := fs.String("format", "csv", "output format: csv or tsv")
	output := fs.String("output", "", "output file path, defaults to stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *databaseID == "" {
		return fmt.Errorf("export-db requires --database")
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	var out io.Writer = stdout
	var file *os.File
	if *output != "" {
		outputPath, err := config.ExpandPath(*output)
		if err != nil {
			return err
		}
		file, err = os.Create(outputPath)
		if err != nil {
			return err
		}
		defer file.Close()
		out = file
	}
	s, err := tableexport.Exporter{Store: st}.Export(ctx, *databaseID, tableexport.Format(*format), out)
	if err != nil {
		return err
	}
	if *output != "" {
		fmt.Fprintf(stdout, "exported %d rows and %d columns from %s to %s\n", s.Rows, s.Columns, s.Database, file.Name())
	}
	return nil
}

func runSearch(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("search query required")
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	results, err := st.Search(ctx, strings.Join(args, " "), 20)
	if err != nil {
		return err
	}
	for _, r := range results {
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\n", r.Kind, r.ID, r.Title, r.Text)
	}
	return nil
}

func runSQL(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("sql query required")
	}
	query := strings.TrimSpace(strings.Join(args, " "))
	if !isReadOnlyQuery(query) {
		return fmt.Errorf("only read-only select/with/pragma queries are allowed")
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	rows, err := st.DB().QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	return printRows(stdout, rows)
}

func runPublish(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	remote := fs.String("remote", cfg.Share.Remote, "git remote")
	repo := fs.String("repo", cfg.Share.RepoPath, "share repo path")
	branch := fs.String("branch", cfg.Share.Branch, "share branch")
	message := fs.String("message", "archive: notcrawl snapshot", "commit message")
	push := fs.Bool("push", false, "push after commit")
	noCommit := fs.Bool("no-commit", false, "write snapshot without committing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if _, err := (markdown.Exporter{Store: st, Dir: cfg.MarkdownDir}).Export(ctx); err != nil {
		return err
	}
	s, err := share.Publish(ctx, st, share.PublishOptions{
		RepoPath: *repo, Remote: *remote, Branch: *branch, MarkdownDir: cfg.MarkdownDir,
		Message: *message, Push: *push, Commit: !*noCommit,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "published %d tables to %s committed=%t pushed=%t\n", len(s.Manifest.Tables), *repo, s.Committed, s.Pushed)
	return nil
}

func runSubscribe(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("subscribe", flag.ContinueOnError)
	repo := fs.String("repo", cfg.Share.RepoPath, "share repo path")
	branch := fs.String("branch", cfg.Share.Branch, "share branch")
	if err := fs.Parse(args); err != nil {
		return err
	}
	remote := cfg.Share.Remote
	if fs.NArg() > 0 {
		remote = fs.Arg(0)
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	manifest, err := share.Subscribe(ctx, st, remote, *repo, *branch)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "subscribed %s tables=%d generated_at=%s\n", remote, len(manifest.Tables), manifest.GeneratedAt)
	return nil
}

func runUpdate(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	repo := fs.String("repo", cfg.Share.RepoPath, "share repo path")
	branch := fs.String("branch", cfg.Share.Branch, "share branch")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	manifest, err := share.Update(ctx, st, *repo, *branch)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "updated tables=%d generated_at=%s\n", len(manifest.Tables), manifest.GeneratedAt)
	return nil
}

func printRows(w io.Writer, rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	fmt.Fprintln(w, strings.Join(cols, "\t"))
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		for i, value := range values {
			if i > 0 {
				fmt.Fprint(w, "\t")
			}
			switch x := value.(type) {
			case nil:
				fmt.Fprint(w, "")
			case []byte:
				fmt.Fprint(w, string(x))
			default:
				fmt.Fprint(w, x)
			}
		}
		fmt.Fprintln(w)
	}
	return rows.Err()
}

func isReadOnlyQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	return strings.HasPrefix(lower, "select ") || strings.HasPrefix(lower, "with ") || strings.HasPrefix(lower, "pragma ")
}

func printHelp(w io.Writer) {
	fmt.Fprint(w, `Usage of notcrawl:
  notcrawl [global flags] <command> [args]

Global flags:
  --config PATH   config file path
  --db PATH       database path override

Commands:
  init                      Write a starter config
  doctor                    Check config, database, desktop cache, and token
  sync --source desktop     Ingest Notion Desktop cache
  sync --source api         Ingest through the official Notion API
  sync --source all         Run enabled sources
  export-md                 Render normalized Markdown from SQLite
  databases                 List crawled Notion databases
  export-db --database ID   Export a database as CSV or TSV
  search QUERY              Search page text
  sql QUERY                 Run read-only SQL
  publish [--push]          Export data and Markdown into a git share repo
  subscribe REMOTE          Clone/import a git share repo
  update                    Pull/import a git share repo
`)
}
