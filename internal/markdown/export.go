package markdown

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/vincentkoc/notcrawl/internal/notiontext"
	"github.com/vincentkoc/notcrawl/internal/store"
)

type Exporter struct {
	Store *store.Store
	Dir   string
}

type Summary struct {
	Pages int
	Files []string
}

func (e Exporter) Export(ctx context.Context) (Summary, error) {
	if e.Store == nil {
		return Summary{}, fmt.Errorf("missing store")
	}
	if e.Dir == "" {
		return Summary{}, fmt.Errorf("missing markdown dir")
	}
	if err := os.MkdirAll(e.Dir, 0o755); err != nil {
		return Summary{}, err
	}
	pages, err := e.Store.Pages(ctx)
	if err != nil {
		return Summary{}, err
	}
	var s Summary
	keep := map[string]bool{}
	for _, page := range pages {
		path, err := e.writePage(ctx, page)
		if err != nil {
			return s, err
		}
		keep[filepath.Clean(path)] = true
		s.Pages++
		s.Files = append(s.Files, path)
	}
	if err := pruneStaleMarkdown(e.Dir, keep); err != nil {
		return s, err
	}
	return s, nil
}

func (e Exporter) writePage(ctx context.Context, page store.Page) (string, error) {
	spaceName, err := e.Store.SpaceName(ctx, page.SpaceID)
	if err != nil {
		return "", err
	}
	teamID, err := e.Store.PageTeamID(ctx, page)
	if err != nil {
		return "", err
	}
	teamName, err := e.Store.TeamName(ctx, teamID)
	if err != nil {
		return "", err
	}
	blocks, err := e.Store.PageBlocks(ctx, page.ID)
	if err != nil {
		return "", err
	}
	comments, err := e.Store.PageComments(ctx, page.ID)
	if err != nil {
		return "", err
	}
	spaceSlug := notiontext.Slug(spaceName)
	titleSlug := maxSlug(notiontext.Slug(page.Title), 96)
	name := fmt.Sprintf("%s-%s.md", titleSlug, notiontext.ShortID(page.ID))
	parts := []string{e.Dir, spaceSlug}
	if teamName != "" {
		parts = append(parts, notiontext.Slug(teamName))
	}
	parts = append(parts, name)
	path := filepath.Join(parts...)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	var b strings.Builder
	writeFrontMatter(&b, page, spaceName, teamID, teamName)
	if page.Title != "" {
		fmt.Fprintf(&b, "# %s\n\n", notiontext.MarkdownEscape(page.Title))
	}
	renderBlocks(&b, page.ID, blocks)
	if len(comments) > 0 {
		if !strings.HasSuffix(b.String(), "\n\n") {
			b.WriteString("\n")
		}
		b.WriteString("## Comments\n\n")
		for _, c := range comments {
			text := notiontext.MarkdownEscape(c.Text)
			if text == "" {
				continue
			}
			fmt.Fprintf(&b, "- %s\n", text)
		}
	}
	out := strings.TrimRight(b.String(), " \n") + "\n"
	return path, os.WriteFile(path, []byte(out), 0o644)
}

func writeFrontMatter(b *strings.Builder, page store.Page, spaceName, teamID, teamName string) {
	b.WriteString("---\n")
	writeKV(b, "id", page.ID)
	writeKV(b, "space_id", page.SpaceID)
	writeKV(b, "space", spaceName)
	writeKV(b, "team_id", teamID)
	writeKV(b, "team", teamName)
	writeKV(b, "title", page.Title)
	writeKV(b, "source", page.Source)
	writeKV(b, "notion_url", page.URL)
	writeKV(b, "created_time", formatMS(page.CreatedTime))
	writeKV(b, "last_edited_time", formatMS(page.LastEditedTime))
	b.WriteString("---\n\n")
}

func writeKV(b *strings.Builder, key, value string) {
	if value == "" {
		return
	}
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, `"`, `\"`)
	fmt.Fprintf(b, "%s: \"%s\"\n", key, value)
}

func renderBlocks(b *strings.Builder, pageID string, blocks []store.Block) {
	children := map[string][]store.Block{}
	for _, block := range blocks {
		if block.ID == pageID {
			continue
		}
		parent := block.ParentID
		children[parent] = append(children[parent], block)
	}
	for parent := range children {
		sort.SliceStable(children[parent], func(i, j int) bool {
			a, z := children[parent][i], children[parent][j]
			if a.DisplayOrder != z.DisplayOrder {
				return a.DisplayOrder < z.DisplayOrder
			}
			if a.CreatedTime == z.CreatedTime {
				return a.ID < z.ID
			}
			return a.CreatedTime < z.CreatedTime
		})
	}
	renderChildren(b, pageID, children, 0)
	if len(children[pageID]) == 0 {
		var loose []store.Block
		for _, block := range blocks {
			if block.ID != pageID && block.ParentID != pageID {
				loose = append(loose, block)
			}
		}
		for _, block := range loose {
			renderBlock(b, block, 0)
		}
	}
}

func renderChildren(b *strings.Builder, parentID string, children map[string][]store.Block, depth int) {
	for _, block := range children[parentID] {
		renderBlock(b, block, depth)
		renderChildren(b, block.ID, children, depth+1)
	}
}

func renderBlock(b *strings.Builder, block store.Block, depth int) {
	text := notiontext.MarkdownEscape(block.Text)
	indent := strings.Repeat("  ", depth)
	switch block.Type {
	case "header", "heading_1":
		writeLine(b, "# "+text)
	case "sub_header", "heading_2":
		writeLine(b, "## "+text)
	case "sub_sub_header", "heading_3":
		writeLine(b, "### "+text)
	case "bulleted_list", "bulleted_list_item":
		writeLine(b, indent+"- "+fallback(text, block.Type))
	case "numbered_list", "numbered_list_item":
		writeLine(b, indent+"1. "+fallback(text, block.Type))
	case "to_do", "to_do_item":
		writeLine(b, indent+"- [ ] "+fallback(text, block.Type))
	case "quote":
		writeLine(b, "> "+fallback(text, block.Type))
	case "code":
		b.WriteString("```text\n")
		b.WriteString(text)
		b.WriteString("\n```\n\n")
	case "divider":
		writeLine(b, "---")
	case "image", "file", "pdf", "video", "figma", "drive":
		writeLine(b, fmt.Sprintf("[%s: %s]", block.Type, fallback(text, block.ID)))
	case "column", "column_list", "table", "table_row", "collection_view":
		if text != "" {
			writeLine(b, text)
		}
	default:
		if text != "" {
			writeLine(b, text)
		} else if block.Type != "" {
			writeLine(b, fmt.Sprintf("[%s]", block.Type))
		}
	}
}

func writeLine(b *strings.Builder, line string) {
	line = strings.TrimRight(line, " ")
	if line == "" {
		return
	}
	b.WriteString(line)
	b.WriteString("\n\n")
}

func fallback(s, fallback string) string {
	if strings.TrimSpace(s) != "" {
		return s
	}
	return fallback
}

func pruneStaleMarkdown(root string, keep map[string]bool) error {
	var dirs []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		path = filepath.Clean(path)
		if d.IsDir() {
			if path != filepath.Clean(root) {
				dirs = append(dirs, path)
			}
			return nil
		}
		if filepath.Ext(path) == ".md" && !keep[path] {
			return os.Remove(path)
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(dirs, func(i, j int) bool {
		return len(dirs[i]) > len(dirs[j])
	})
	for _, dir := range dirs {
		if err := os.Remove(dir); err != nil && !isIgnorableRemoveDirError(err) {
			return err
		}
	}
	return nil
}

func isIgnorableRemoveDirError(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST)
}

func formatMS(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func maxSlug(s string, max int) string {
	if len(s) <= max {
		return s
	}
	s = strings.TrimRight(s[:max], "-")
	if s == "" {
		return "untitled"
	}
	return s
}
