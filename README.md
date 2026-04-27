<img src="docs/notcrawl_banner.jpg" alt="notcrawl banner"/>

# 🗞️ notcrawl

`notcrawl` mirrors Notion workspace data into local SQLite and normalized
Markdown so you can search, query, diff, and share your Notion memory without
depending on the Notion UI.

It has two ingestion paths:

- `desktop`: read-only snapshots of the local Notion desktop cache
- `api`: official Notion API sync with rate-limit aware crawling

SQLite is the canonical archive. Markdown is the durable human/agent surface.
Git share mode publishes normalized snapshots that other machines can subscribe
to without holding Notion credentials.

## Current Scope

- local SQLite storage with FTS5
- read-only local desktop cache ingestion from macOS Notion
- official API page/block/user/comment ingestion
- Notion database metadata and row ingestion through the official API
- current Notion data-source API support plus legacy database endpoint support
- normalized Markdown export organized by Unicode-safe workspace, teamspace, and page paths
- CSV/TSV export for crawled Notion database rows
- compressed JSONL git-share snapshots plus import/update workflows
- archive status, activity reporting, and SQLite maintenance commands
- read-only SQL access for ad hoc inspection

## Install

```bash
brew tap vincentkoc/tap
brew install notcrawl
```

You can also download archives, `.deb`, or `.rpm` packages from the
[latest release](https://github.com/vincentkoc/notcrawl/releases/latest).

## Quick Start

Use the local Notion Desktop cache:

```bash
notcrawl init
notcrawl doctor
notcrawl status
notcrawl report
notcrawl sync --source desktop
notcrawl export-md
notcrawl search "launch plan"
```

Or use the official Notion API:

```bash
export NOTION_TOKEN="secret_..."
notcrawl sync --source api
notcrawl databases
notcrawl export-db --database DATABASE_ID --format csv --output roadmap.csv
```

Default paths:

- config: `~/.notcrawl/config.toml`
- database: `~/.notcrawl/notcrawl.db`
- cache: `~/.notcrawl/cache`
- Markdown archive: `~/.notcrawl/pages`
- git share repo: `~/.notcrawl/share`

## Commands

- `init` writes a starter config
- `doctor` checks config, SQLite, desktop cache, and token presence
- `status` prints archive counts, last sync time, and database/WAL size
- `report` summarizes recent page, database, space, and comment activity
- `maintain` rebuilds FTS, optimizes SQLite indexes, and can run `VACUUM`
- `sync` ingests from `desktop`, `api`, or `all`
- `export-md` renders normalized Markdown files from SQLite
- `databases` lists crawled Notion databases
- `export-db` exports a crawled Notion database to CSV or TSV
- `search` searches page and comment text through FTS5
- `sql` runs read-only SQL against the archive
- `publish` exports SQLite tables and Markdown into a git share repo
- `subscribe` clones a share repo and imports the latest snapshot
- `update` pulls and imports a subscribed share repo

## Distribution

Release packaging is managed with GoReleaser. Tagged releases build tarballs,
checksums, `.deb`, `.rpm`, GitHub release notes, and a Homebrew tap update.

See [`docs/distribution.md`](docs/distribution.md) for release operations.

## Safety Model

Desktop mode is read-only. It snapshots Notion's local SQLite database before
reading it and never writes to Notion application storage.

API mode uses the official Notion API. It stores raw API payloads alongside
normalized rows so renderers can improve without recrawling.

Secrets are never exported into Markdown or git-share snapshots.
