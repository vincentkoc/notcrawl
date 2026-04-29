package notiontext

import "testing"

func TestTitleFromProperties(t *testing.T) {
	got := TitleFromProperties(`{"title":[["Launch Plan"]]}`)
	if got != "Launch Plan" {
		t.Fatalf("got %q", got)
	}
}

func TestTitleFromPropertiesPrefersNotionRichTextOnce(t *testing.T) {
	got := TitleFromProperties(`{
		"Name": {
			"id": "title",
			"type": "title",
			"title": [{
				"type": "text",
				"plain_text": "OpenClaw",
				"text": {"content": "OpenClaw"}
			}]
		}
	}`)
	if got != "OpenClaw" {
		t.Fatalf("got %q", got)
	}
}

func TestPlainPrefersNotionRichTextPlainTextOnce(t *testing.T) {
	got := Plain([]any{map[string]any{
		"type":       "text",
		"plain_text": "OpenClaw",
		"text": map[string]any{
			"content": "OpenClaw",
		},
	}})
	if got != "OpenClaw" {
		t.Fatalf("got %q", got)
	}
}

func TestPlainFallsBackToNotionTextContentOnce(t *testing.T) {
	got := Plain([]any{map[string]any{
		"type": "text",
		"text": map[string]any{
			"content": "OpenClaw",
		},
	}})
	if got != "OpenClaw" {
		t.Fatalf("got %q", got)
	}
}

func TestPlainWalksTitleOnlyOnce(t *testing.T) {
	got := Plain(map[string]any{
		"title": []any{map[string]any{
			"plain_text": "Roadmap",
			"text": map[string]any{
				"content": "Roadmap",
			},
		}},
	})
	if got != "Roadmap" {
		t.Fatalf("got %q", got)
	}
}

func TestSlug(t *testing.T) {
	got := Slug("Launch Plan / Q2")
	if got != "launch-plan-q2" {
		t.Fatalf("got %q", got)
	}
}

func TestSlugRemovesEmojiPathText(t *testing.T) {
	got := Slug("研究 🚀 / 計画 ✅")
	if got != "研究-計画" {
		t.Fatalf("got %q", got)
	}
}

func TestSlugRemovesUnsafePathText(t *testing.T) {
	got := Slug(`A/B\C:D*E?F"G<H>I|J`)
	if got != "a-b-c-d-e-f-g-h-i-j" {
		t.Fatalf("got %q", got)
	}
}

func TestSlugRemovesEmojiVariationSelectors(t *testing.T) {
	got := Slug("⚠️ Production Incident Guide")
	if got != "production-incident-guide" {
		t.Fatalf("got %q", got)
	}
}

func TestShortIDKeepsEnoughEntropyForDesktopIDs(t *testing.T) {
	got := ShortID("24f71240-0000-0000-0000-123456789abc")
	if got != "24f71240-56789abc" {
		t.Fatalf("got %q", got)
	}
}
