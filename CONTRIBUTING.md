# Contributing to notcrawl

Keep real Notion workspace data, secrets, tokens, cookies, and exported private
content out of git.

Useful local checks:

```bash
go test ./...
go build ./cmd/notcrawl
```

Implementation notes:

- read Notion Desktop data through snapshots only
- prefer stable normalized rows plus raw source payloads
- keep Markdown rendering deterministic
- add comments only where Notion-specific behavior is not obvious
- keep `README.md`, `SPEC.md`, and examples in sync
