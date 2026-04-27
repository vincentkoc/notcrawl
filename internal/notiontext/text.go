package notiontext

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

var spaceRE = regexp.MustCompile(`\s+`)

func Normalize(s string) string {
	return strings.TrimSpace(spaceRE.ReplaceAllString(s, " "))
}

func PlainFromJSON(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return Normalize(raw)
	}
	return Plain(v)
}

func Plain(v any) string {
	var parts []string
	walk(v, &parts)
	return Normalize(strings.Join(parts, " "))
}

func TitleFromProperties(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return ""
	}
	if m, ok := v.(map[string]any); ok {
		for _, key := range []string{"title", "Name", "name"} {
			if text := Plain(m[key]); text != "" {
				return text
			}
		}
		for _, value := range m {
			if text := Plain(value); text != "" {
				return text
			}
		}
	}
	return Plain(v)
}

func MarkdownEscape(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.TrimRight(s, " \n")
}

func ShortID(id string) string {
	clean := strings.ReplaceAll(id, "-", "")
	if len(clean) > 16 {
		return clean[:8] + "-" + clean[len(clean)-8:]
	}
	if clean == "" {
		return "unknown"
	}
	return clean
}

func Slug(s string) string {
	s = strings.ToLower(Normalize(s))
	var b strings.Builder
	lastDash := false
	wrote := false
	for _, r := range s {
		switch {
		case isSlugRune(r):
			b.WriteRune(r)
			lastDash = false
			wrote = true
		case isSlugSeparator(r):
			if wrote && !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		case unicode.IsControl(r):
			continue
		default:
			if wrote && !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "untitled"
	}
	return out
}

func isSlugRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsMark(r) || (r > unicode.MaxASCII && unicode.IsSymbol(r)) || r == '\u200d'
}

func isSlugSeparator(r rune) bool {
	return unicode.IsSpace(r) || strings.ContainsRune(`-_/.\:;`, r)
}

func MarshalRaw(m map[string]any) string {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf(`{"marshal_error":%q}`, err.Error())
	}
	return string(b)
}

func walk(v any, parts *[]string) {
	switch x := v.(type) {
	case nil:
		return
	case string:
		if x != "" {
			*parts = append(*parts, x)
		}
	case []any:
		for _, item := range x {
			walk(item, parts)
		}
	case map[string]any:
		for _, key := range []string{"plain_text", "content", "text", "name", "title"} {
			if value, ok := x[key]; ok {
				walk(value, parts)
			}
		}
		if rt, ok := x["rich_text"]; ok {
			walk(rt, parts)
		}
		if title, ok := x["title"]; ok {
			walk(title, parts)
		}
		if text, ok := x["text"].(map[string]any); ok {
			walk(text["content"], parts)
		}
	}
}
