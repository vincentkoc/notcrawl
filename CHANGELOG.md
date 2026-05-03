# Changelog

## Unreleased

- Add crawlkit control metadata/status surfaces with `metadata --json`, `status --json`, and `doctor --json`.
- Report primary archive and desktop-cache SQLite inventories in status JSON for shared local control surfaces.
- Add `notcrawl tui`, a local terminal browser for archived pages and databases backed by `crawlkit/tui`.
- Render TUI rows with compact panes so page and database metadata stays in context/detail instead of crowding the row list.
- Resolve database parent names for the TUI parent pane so collection nesting is readable instead of raw IDs.
- Hide noisy block-derived Notion parent labels in the TUI by falling back to the workspace label when parent text contains raw Notion identifiers.
- Normalize workspace-level Notion parents as `Workspace: <name>` so the TUI left pane does not split the same workspace into duplicate parent groups.
- Inherit shared crawlkit TUI improvements for newest-first startup, count-header sorting, preview-first document detail panes, and gitcrawl-style metadata labels.
- Feed longer, block-shaped Notion page previews into the TUI detail pane so pages read more like documents instead of flat metadata.
- Include page comments in Notion TUI previews after block content.
- Route the TUI through read-only SQLite access and cover the JSON fallback in tests.
