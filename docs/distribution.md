# Distribution

`notcrawl` ships through GitHub Releases, Homebrew tap updates, and optional
Cloudsmith APT/RPM repositories.

## Local Checks

```bash
go test ./...
go build ./cmd/notcrawl
make release-notes TAG=v0.1.0
```

If GoReleaser is installed:

```bash
make release-snapshot
```

That creates local snapshot archives, checksums, `.deb`, and `.rpm` packages
under `dist/` without publishing.

## Release Notes

Generate local notes from conventional commits:

```bash
scripts/release-notes.sh v0.1.0
```

GitHub also uses Release Drafter to label PRs and maintain draft release notes.

## Tagged Release

Create and push a semver tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow:

1. runs tests
2. generates release notes
3. builds GoReleaser artifacts
4. publishes GitHub release assets
5. optionally publishes APT/RPM packages to Cloudsmith
6. updates the Homebrew tap

## Required Secrets

- `HOMEBREW_TAP_GITHUB_TOKEN`: token that can push to the tap repository
- `CLOUDSMITH_API_KEY`: optional; enables package publishing

## Optional Variables

- `HOMEBREW_TAP_REPO`: defaults to `vincentkoc/tap`
- `CLOUDSMITH_APT_TARGETS`: comma-separated targets like `ubuntu/jammy,debian/trixie`
- `CLOUDSMITH_DISTRIBUTION` and `CLOUDSMITH_RELEASE`: legacy single APT target
- `CLOUDSMITH_RPM_DISTRIBUTION`: defaults to `el`
- `CLOUDSMITH_RPM_RELEASE`: defaults to `9`

## Manual Reruns

If Cloudsmith publish fails after GitHub release assets exist:

```bash
gh workflow run publish-apt.yml -f tag_name=v0.1.0
gh workflow run publish-rpm.yml -f tag_name=v0.1.0
```

If the Homebrew tap update fails:

```bash
gh workflow run homebrew-tap.yml -f tag_name=v0.1.0
```
