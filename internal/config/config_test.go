package config

import "testing"

func TestDefaultUsesCurrentNotionAPIVersion(t *testing.T) {
	cfg := Default()
	if cfg.Notion.API.Version != "2026-03-11" {
		t.Fatalf("unexpected API version: %s", cfg.Notion.API.Version)
	}
}
