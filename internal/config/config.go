package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	crawlconfig "github.com/vincentkoc/crawlkit/config"
)

const (
	defaultDirName     = ".notcrawl"
	defaultDesktopPath = "~/Library/Application Support/Notion/notion.db"
	defaultAPIVersion  = "2026-03-11"
)

type Config struct {
	DBPath      string       `toml:"db_path"`
	CacheDir    string       `toml:"cache_dir"`
	MarkdownDir string       `toml:"markdown_dir"`
	Notion      NotionConfig `toml:"notion"`
	Share       ShareConfig  `toml:"share"`
}

type NotionConfig struct {
	Desktop DesktopConfig `toml:"desktop"`
	API     APIConfig     `toml:"api"`
}

type DesktopConfig struct {
	Enabled bool   `toml:"enabled"`
	Path    string `toml:"path"`
}

type APIConfig struct {
	Enabled  bool   `toml:"enabled"`
	TokenEnv string `toml:"token_env"`
	BaseURL  string `toml:"base_url"`
	Version  string `toml:"version"`
}

type ShareConfig struct {
	Remote     string `toml:"remote"`
	Branch     string `toml:"branch"`
	RepoPath   string `toml:"repo_path"`
	StaleAfter string `toml:"stale_after"`
}

var appConfig = crawlconfig.App{Name: "notcrawl", BaseDir: "~/" + defaultDirName, LegacyBaseDir: "~/" + defaultDirName}

func Default() Config {
	paths, err := appConfig.DefaultPaths()
	if err != nil {
		base := filepath.ToSlash(filepath.Join("~", defaultDirName))
		paths = crawlconfig.Paths{
			DBPath:   filepath.ToSlash(filepath.Join(base, "notcrawl.db")),
			CacheDir: filepath.ToSlash(filepath.Join(base, "cache")),
			ShareDir: filepath.ToSlash(filepath.Join(base, "share")),
		}
	}
	return Config{
		DBPath:      filepath.ToSlash(paths.DBPath),
		CacheDir:    filepath.ToSlash(paths.CacheDir),
		MarkdownDir: filepath.ToSlash(filepath.Join(paths.BaseDir, "pages")),
		Notion: NotionConfig{
			Desktop: DesktopConfig{Enabled: true, Path: ""},
			API: APIConfig{
				Enabled:  true,
				TokenEnv: "NOTION_TOKEN",
				BaseURL:  "https://api.notion.com/v1",
				Version:  defaultAPIVersion,
			},
		},
		Share: ShareConfig{
			Branch:     "main",
			RepoPath:   filepath.ToSlash(paths.ShareDir),
			StaleAfter: "1h",
		},
	}
}

func DefaultPath() (string, error) {
	paths, err := appConfig.DefaultPaths()
	return paths.ConfigPath, err
}

func Load(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return Config{}, err
		}
	}
	path, err := ExpandPath(path)
	if err != nil {
		return Config{}, err
	}
	cfg := Default()
	if err := crawlconfig.LoadTOML(path, &cfg); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := cfg.Resolve(); err != nil {
				return Config{}, err
			}
			return cfg, nil
		}
		return Config{}, err
	}
	if err := cfg.Resolve(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func WriteStarter(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return "", err
		}
	}
	path, err := ExpandPath(path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if _, err := os.Stat(path); err == nil {
		return path, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	cfg := Default()
	return path, crawlconfig.WriteTOML(path, cfg, 0o600)
}

func (c *Config) Resolve() error {
	if strings.TrimSpace(c.Notion.Desktop.Path) == "" {
		c.Notion.Desktop.Path = defaultDesktopPath
	}
	if strings.TrimSpace(c.Notion.API.TokenEnv) == "" {
		c.Notion.API.TokenEnv = "NOTION_TOKEN"
	}
	if strings.TrimSpace(c.Notion.API.BaseURL) == "" {
		c.Notion.API.BaseURL = "https://api.notion.com/v1"
	}
	if strings.TrimSpace(c.Notion.API.Version) == "" {
		c.Notion.API.Version = defaultAPIVersion
	}
	if strings.TrimSpace(c.Share.Branch) == "" {
		c.Share.Branch = "main"
	}
	if strings.TrimSpace(c.Share.StaleAfter) == "" {
		c.Share.StaleAfter = "1h"
	}
	if _, err := time.ParseDuration(c.Share.StaleAfter); err != nil {
		return fmt.Errorf("invalid share stale_after: %w", err)
	}
	paths := []*string{&c.DBPath, &c.CacheDir, &c.MarkdownDir, &c.Notion.Desktop.Path, &c.Share.RepoPath}
	for _, p := range paths {
		expanded, err := ExpandPath(*p)
		if err != nil {
			return err
		}
		*p = expanded
	}
	return nil
}

func ExpandPath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	return filepath.Abs(crawlconfig.ExpandHome(path))
}

func (c Config) APIToken() string {
	return os.Getenv(c.Notion.API.TokenEnv)
}
