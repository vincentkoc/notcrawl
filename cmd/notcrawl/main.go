package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vincentkoc/crawlkit/control"
	"github.com/vincentkoc/crawlkit/tui"
	"github.com/vincentkoc/notcrawl/internal/config"
	"github.com/vincentkoc/notcrawl/internal/markdown"
	"github.com/vincentkoc/notcrawl/internal/notionapi"
	"github.com/vincentkoc/notcrawl/internal/notiondesktop"
	"github.com/vincentkoc/notcrawl/internal/notiontext"
	"github.com/vincentkoc/notcrawl/internal/report"
	"github.com/vincentkoc/notcrawl/internal/share"
	"github.com/vincentkoc/notcrawl/internal/store"
	"github.com/vincentkoc/notcrawl/internal/tableexport"
)

var version = "dev"

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "notcrawl:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "--version" {
		fmt.Fprintln(stdout, version)
		return nil
	}
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
	if cmd == "version" {
		fmt.Fprintln(stdout, version)
		return nil
	}
	if cmd == "metadata" {
		return runMetadata(stdout)
	}
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
		return runDoctor(ctx, stdout, cfg, cmdArgs)
	case "status":
		return runStatus(ctx, stdout, cfg, cmdArgs)
	case "report":
		return runReport(ctx, stdout, cfg)
	case "maintain":
		return runMaintain(ctx, stdout, cfg, cmdArgs)
	case "sync":
		return runSync(ctx, stdout, cfg, cmdArgs)
	case "tap":
		return runSync(ctx, stdout, cfg, []string{"--source", "desktop"})
	case "export-md":
		return runExportMarkdown(ctx, stdout, cfg)
	case "databases":
		return runDatabases(ctx, stdout, cfg)
	case "export-db":
		return runExportDatabase(ctx, stdout, cfg, cmdArgs)
	case "search":
		return runSearch(ctx, stdout, cfg, cmdArgs)
	case "tui":
		return runTUI(ctx, stdout, cfg, cmdArgs)
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

func runDoctor(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	jsonOut := fs.Bool("json", false, "print doctor JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("doctor takes flags only")
	}
	_ = jsonOut
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
	status := store.Status{DBPath: cfg.DBPath}
	st, err := store.OpenReadOnly(cfg.DBPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		defer st.Close()
		status, err = st.Status(ctx)
		if err != nil {
			return err
		}
	}
	report["status"] = status
	report["api_version"] = cfg.Notion.API.Version
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, string(b))
	return nil
}

func runStatus(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	jsonOut := fs.Bool("json", false, "print normalized crawlkit status JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("status takes flags only")
	}
	status := store.Status{DBPath: cfg.DBPath}
	st, err := store.OpenReadOnly(cfg.DBPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		defer st.Close()
		status, err = st.Status(ctx)
		if err != nil {
			return err
		}
	}
	if *jsonOut {
		return writeJSON(stdout, controlStatus(cfg, status))
	}
	b, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, string(b))
	return nil
}

func runMetadata(stdout io.Writer) error {
	defaults := config.Default()
	configPath, err := config.DefaultPath()
	if err != nil {
		return err
	}
	manifest := control.NewManifest("notcrawl", "Notion Crawl", "notcrawl")
	manifest.Description = "Local-first Notion archive crawler."
	manifest.Branding = control.Branding{SymbolName: "doc.text.magnifyingglass", AccentColor: "#111111", BundleIdentifier: "notion.id"}
	manifest.Paths = control.Paths{
		DefaultConfig:   configPath,
		ConfigEnv:       "NOTCRAWL_CONFIG",
		DefaultDatabase: defaults.DBPath,
		DefaultCache:    defaults.CacheDir,
		DefaultLogs:     filepath.Join(filepath.Dir(defaults.DBPath), "logs"),
		DefaultShare:    defaults.Share.RepoPath,
	}
	manifest.Capabilities = []string{"metadata", "status", "doctor", "sync", "tap", "tui", "git-share", "sql", "markdown", "table-export"}
	manifest.Privacy = control.Privacy{ContainsPrivateMessages: false, ExportsSecrets: false, LocalOnlyScopes: []string{"notion", "desktop-cache", "sqlite", "git-share"}}
	manifest.Commands = map[string]control.Command{
		"status":    {Title: "Status", Argv: []string{"notcrawl", "status", "--json"}, JSON: true},
		"doctor":    {Title: "Doctor", Argv: []string{"notcrawl", "doctor", "--json"}, JSON: true},
		"sync":      {Title: "Sync", Argv: []string{"notcrawl", "sync", "--source", "all"}, JSON: true, Mutates: true},
		"tap":       {Title: "Import desktop cache", Argv: []string{"notcrawl", "sync", "--source", "desktop"}, JSON: true, Mutates: true},
		"tui":       {Title: "Terminal browser", Argv: []string{"notcrawl", "tui"}},
		"tui-json":  {Title: "Terminal browser rows", Argv: []string{"notcrawl", "tui", "--json"}, JSON: true},
		"publish":   {Title: "Publish share", Argv: []string{"notcrawl", "publish"}, JSON: true, Mutates: true},
		"subscribe": {Title: "Subscribe share", Argv: []string{"notcrawl", "subscribe"}, JSON: true, Mutates: true},
		"update":    {Title: "Update share", Argv: []string{"notcrawl", "update"}, JSON: true, Mutates: true},
		"export-md": {Title: "Export Markdown", Argv: []string{"notcrawl", "export-md"}, Mutates: true},
		"databases": {Title: "List databases", Argv: []string{"notcrawl", "databases"}},
		"export-db": {Title: "Export database", Argv: []string{"notcrawl", "export-db"}, Mutates: true},
		"legacy-db": {Title: "Legacy database override", Argv: []string{"notcrawl", "--db"}, Legacy: true},
	}
	return writeJSON(stdout, manifest)
}

func writeJSON(stdout io.Writer, value any) error {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func controlStatus(cfg config.Config, status store.Status) control.Status {
	counts := []control.Count{
		control.NewCount("spaces", "Spaces", int64(status.Spaces)),
		control.NewCount("users", "Users", int64(status.Users)),
		control.NewCount("teams", "Teams", int64(status.Teams)),
		control.NewCount("pages", "Pages", int64(status.Pages)),
		control.NewCount("blocks", "Blocks", int64(status.Blocks)),
		control.NewCount("collections", "Databases", int64(status.Collections)),
		control.NewCount("comments", "Comments", int64(status.Comments)),
		control.NewCount("raw_records", "Raw records", int64(status.RawRecords)),
	}
	out := control.NewStatus("notcrawl", fmt.Sprintf("%d pages and %d databases", status.Pages, status.Collections))
	out.State = "current"
	out.DatabasePath = status.DBPath
	out.DatabaseBytes = status.DBBytes
	out.WALBytes = status.WALBytes
	out.Counts = counts
	if status.LastSyncAt > 0 {
		out.LastSyncAt = time.UnixMilli(status.LastSyncAt).UTC().Format(time.RFC3339)
	}
	out.Share = &control.Share{Enabled: cfg.Share.Remote != "" || cfg.Share.RepoPath != "", RepoPath: cfg.Share.RepoPath, Remote: cfg.Share.Remote, Branch: cfg.Share.Branch}
	out.Databases = append(out.Databases, control.SQLiteDatabase("primary", "Notion archive", "archive", status.DBPath, true, counts))
	out.Databases = append(out.Databases, desktopCacheDatabases(cfg.CacheDir)...)
	return out
}

func desktopCacheDatabases(cacheDir string) []control.Database {
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return nil
	}
	var out []control.Database
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".db") {
			continue
		}
		path := filepath.Join(cacheDir, entry.Name())
		out = append(out, control.SQLiteDatabase("cache-"+strings.TrimSuffix(entry.Name(), ".db"), entry.Name(), "desktop-cache", path, false, nil))
	}
	return out
}

func runReport(ctx context.Context, stdout io.Writer, cfg config.Config) error {
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	activity, err := report.Build(ctx, st, report.Options{})
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(activity, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, string(b))
	return nil
}

func runMaintain(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("maintain", flag.ContinueOnError)
	vacuum := fs.Bool("vacuum", false, "run VACUUM after rebuilding and optimizing indexes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	summary, err := st.Optimize(ctx, *vacuum)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(summary, "", "  ")
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
		fmt.Fprintf(stdout, "desktop: pages=%d blocks=%d teams=%d collections=%d comments=%d snapshot=%s\n", s.Pages, s.Blocks, s.Teams, s.Collections, s.Comments, s.Source.Snapshot)
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
			fmt.Fprintf(stdout, "desktop: pages=%d blocks=%d teams=%d collections=%d comments=%d snapshot=%s\n", s.Pages, s.Blocks, s.Teams, s.Collections, s.Comments, s.Source.Snapshot)
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
	all := fs.Bool("all", false, "export every crawled database")
	dir := fs.String("dir", "", "directory for --all exports")
	format := fs.String("format", "csv", "output format: csv or tsv")
	output := fs.String("output", "", "output file path, defaults to stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *all {
		if *databaseID != "" {
			return fmt.Errorf("export-db cannot combine --all and --database")
		}
		if *output != "" {
			return fmt.Errorf("export-db cannot combine --all and --output")
		}
		if *dir == "" {
			return fmt.Errorf("export-db --all requires --dir")
		}
		return runExportAllDatabases(ctx, stdout, cfg, tableexport.Format(*format), *dir)
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

func runExportAllDatabases(ctx context.Context, stdout io.Writer, cfg config.Config, format tableexport.Format, dir string) error {
	ext, err := exportExtension(format)
	if err != nil {
		return err
	}
	dir, err = config.ExpandPath(dir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	collections, err := st.Collections(ctx)
	if err != nil {
		return err
	}
	index, err := os.Create(filepath.Join(dir, "index.tsv"))
	if err != nil {
		return err
	}
	fmt.Fprintln(index, "id\tname\tsource\trows\tcolumns\tfile")
	exporter := tableexport.Exporter{Store: st}
	used := map[string]bool{}
	var databases, rows int
	for _, collection := range collections {
		name := exportDatabaseFilename(collection, ext, used)
		path := filepath.Join(dir, name)
		file, err := os.Create(path)
		if err != nil {
			_ = index.Close()
			return err
		}
		s, exportErr := exporter.Export(ctx, collection.ID, format, file)
		closeErr := file.Close()
		if exportErr != nil {
			_ = index.Close()
			return exportErr
		}
		if closeErr != nil {
			_ = index.Close()
			return closeErr
		}
		databases++
		rows += s.Rows
		fmt.Fprintf(index, "%s\t%s\t%s\t%d\t%d\t%s\n", collection.ID, collection.Name, collection.Source, s.Rows, s.Columns, name)
	}
	if err := index.Close(); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "exported %d databases and %d rows to %s\n", databases, rows, dir)
	return nil
}

func exportExtension(format tableexport.Format) (string, error) {
	switch format {
	case "", tableexport.FormatCSV:
		return "csv", nil
	case tableexport.FormatTSV:
		return "tsv", nil
	default:
		return "", fmt.Errorf("unsupported format %q", format)
	}
}

func exportDatabaseFilename(collection store.Collection, ext string, used map[string]bool) string {
	baseName := collection.Name
	if strings.TrimSpace(baseName) == "" {
		baseName = collection.ID
	}
	base := notiontext.Slug(baseName) + "-" + notiontext.ShortID(collection.ID)
	name := base + "." + ext
	for i := 2; used[name]; i++ {
		name = fmt.Sprintf("%s-%d.%s", base, i, ext)
	}
	used[name] = true
	return name
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
		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\n", searchField(r.Kind), searchField(r.ID), searchField(r.Title), searchField(r.Text))
	}
	return nil
}

func runTUI(ctx context.Context, stdout io.Writer, cfg config.Config, args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	limit := fs.Int("limit", 200, "maximum rows to load")
	kind := fs.String("kind", "all", "rows to browse: all, pages, databases")
	jsonOut := fs.Bool("json", false, "print browser rows as JSON instead of opening the terminal UI")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("tui takes flags only")
	}
	if *limit <= 0 {
		return fmt.Errorf("tui --limit must be positive")
	}
	rows, err := tuiRows(ctx, cfg, *kind, *limit)
	if err != nil {
		return err
	}
	return tui.Browse(ctx, tui.BrowseOptions{
		AppName:        "notcrawl",
		Title:          "notcrawl archive",
		EmptyMessage:   "notcrawl has no local pages or databases yet",
		Rows:           rows,
		JSON:           *jsonOut,
		Layout:         tui.LayoutDocument,
		SourceKind:     archiveSourceKind(cfg),
		SourceLocation: archiveSourceLocation(cfg),
		Stdout:         stdout,
	})
}

func archiveSourceKind(cfg config.Config) string {
	if strings.TrimSpace(cfg.Share.Remote) != "" {
		return tui.SourceRemote
	}
	return tui.SourceLocal
}

func archiveSourceLocation(cfg config.Config) string {
	if strings.TrimSpace(cfg.Share.Remote) != "" {
		return cfg.Share.Remote
	}
	return cfg.DBPath
}

func tuiRows(ctx context.Context, cfg config.Config, kind string, limit int) ([]tui.Row, error) {
	st, err := store.OpenReadOnly(cfg.DBPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []tui.Row{}, nil
		}
		return nil, err
	}
	defer st.Close()
	var rows []tui.Row
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "all":
		pages, err := st.Pages(ctx)
		if err != nil {
			return nil, err
		}
		rows = append(rows, pageTUIRows(pages, limit)...)
		if len(rows) < limit {
			collections, err := st.Collections(ctx)
			if err != nil {
				return nil, err
			}
			rows = append(rows, collectionTUIRows(collections, limit-len(rows))...)
		}
	case "pages", "page":
		pages, err := st.Pages(ctx)
		if err != nil {
			return nil, err
		}
		rows = append(rows, pageTUIRows(pages, limit)...)
	case "databases", "database", "collections", "collection":
		collections, err := st.Collections(ctx)
		if err != nil {
			return nil, err
		}
		rows = append(rows, collectionTUIRows(collections, limit)...)
	default:
		return nil, fmt.Errorf("unknown tui kind %q", kind)
	}
	return rows, nil
}

func pageTUIRows(pages []store.Page, limit int) []tui.Row {
	if limit > len(pages) {
		limit = len(pages)
	}
	items := make([]tui.Row, 0, limit)
	for _, page := range pages[:limit] {
		title := strings.TrimSpace(page.Title)
		if title == "" {
			title = page.ID
		}
		items = append(items, tui.Row{
			Source:    "notion",
			Kind:      "page",
			ID:        page.ID,
			ParentID:  strings.Trim(page.ParentTable+":"+page.ParentID, ":"),
			Scope:     page.SpaceID,
			Container: page.CollectionID,
			Title:     title,
			URL:       page.URL,
			UpdatedAt: formatMillis(page.LastEditedTime),
			Tags:      []string{page.Source},
			Fields: map[string]string{
				"parent_table": page.ParentTable,
				"source":       page.Source,
			},
		})
	}
	return items
}

func collectionTUIRows(collections []store.Collection, limit int) []tui.Row {
	if limit > len(collections) {
		limit = len(collections)
	}
	items := make([]tui.Row, 0, limit)
	for _, collection := range collections[:limit] {
		title := strings.TrimSpace(collection.Name)
		if title == "" {
			title = collection.ID
		}
		items = append(items, tui.Row{
			Source:   "notion",
			Kind:     "database",
			ID:       collection.ID,
			ParentID: strings.Trim(collection.ParentTable+":"+collection.ParentID, ":"),
			Scope:    collection.SpaceID,
			Title:    title,
			Tags:     []string{collection.Source},
			Fields: map[string]string{
				"parent_table": collection.ParentTable,
				"source":       collection.Source,
			},
		})
	}
	return items
}

func formatMillis(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func searchField(s string) string {
	return strings.Join(strings.Fields(s), " ")
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
  --version       print version and exit

Commands:
  metadata                  Print crawlkit control metadata
  version                   Print version
  init                      Write a starter config
  doctor                    Check config, database, desktop cache, and token
  status                    Show archive counts and database size
  report                    Show recent archive activity
  maintain [--vacuum]       Rebuild FTS and optimize SQLite indexes
  sync --source desktop     Ingest Notion Desktop cache
  sync --source api         Ingest through the official Notion API
  sync --source all         Run enabled sources
  tap                       Legacy-friendly alias for sync --source desktop
  export-md                 Render normalized Markdown from SQLite
  databases                 List crawled Notion databases
  export-db --database ID   Export a database as CSV or TSV
  export-db --all --dir DIR Export every database as CSV or TSV
  search QUERY              Search page text
  tui                       Browse pages and databases in the terminal UI
  sql QUERY                 Run read-only SQL
  publish [--push]          Export data and Markdown into a git share repo
  subscribe REMOTE          Clone/import a git share repo
  update                    Pull/import a git share repo
`)
}
