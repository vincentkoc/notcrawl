package tableexport

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/vincentkoc/notcrawl/internal/notiontext"
	"github.com/vincentkoc/notcrawl/internal/store"
)

type Format string

const (
	FormatCSV Format = "csv"
	FormatTSV Format = "tsv"
)

type Exporter struct {
	Store *store.Store
}

type Summary struct {
	Database string
	Rows     int
	Columns  int
}

func (e Exporter) Export(ctx context.Context, databaseID string, format Format, w io.Writer) (Summary, error) {
	if e.Store == nil {
		return Summary{}, fmt.Errorf("missing store")
	}
	if databaseID == "" {
		return Summary{}, fmt.Errorf("database id is required")
	}
	collection, err := e.Store.Collection(ctx, databaseID)
	if err != nil {
		return Summary{}, err
	}
	pages, err := e.Store.CollectionPages(ctx, databaseID)
	if err != nil {
		return Summary{}, err
	}
	columns := columnsFor(collection, pages)
	writer := csv.NewWriter(w)
	if format == FormatTSV {
		writer.Comma = '\t'
	} else if format != "" && format != FormatCSV {
		return Summary{}, fmt.Errorf("unsupported format %q", format)
	}
	if err := writer.Write(columns); err != nil {
		return Summary{}, err
	}
	for _, page := range pages {
		props := decodeMap(page.PropertiesJSON)
		row := make([]string, 0, len(columns))
		for _, col := range columns {
			switch col {
			case "page_id":
				row = append(row, page.ID)
			case "page_title":
				row = append(row, page.Title)
			case "url":
				row = append(row, page.URL)
			default:
				row = append(row, propertyValueText(props[col]))
			}
		}
		if err := writer.Write(row); err != nil {
			return Summary{}, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return Summary{}, err
	}
	return Summary{Database: collection.ID, Rows: len(pages), Columns: len(columns)}, nil
}

func columnsFor(collection store.Collection, pages []store.Page) []string {
	seen := map[string]bool{"page_id": true, "page_title": true, "url": true}
	cols := []string{"page_id", "page_title", "url"}
	for _, name := range schemaPropertyNames(collection.SchemaJSON) {
		if !seen[name] {
			seen[name] = true
			cols = append(cols, name)
		}
	}
	var extras []string
	for _, page := range pages {
		for name := range decodeMap(page.PropertiesJSON) {
			if !seen[name] {
				seen[name] = true
				extras = append(extras, name)
			}
		}
	}
	sort.Strings(extras)
	return append(cols, extras...)
}

func schemaPropertyNames(raw string) []string {
	props := decodeMap(raw)
	var title []string
	var rest []string
	for name, value := range props {
		m, ok := value.(map[string]any)
		if ok && m["type"] == "title" {
			title = append(title, name)
			continue
		}
		rest = append(rest, name)
	}
	sort.Strings(title)
	sort.Strings(rest)
	return append(title, rest...)
}

func decodeMap(raw string) map[string]any {
	out := map[string]any{}
	if strings.TrimSpace(raw) == "" {
		return out
	}
	_ = json.Unmarshal([]byte(raw), &out)
	return out
}

func propertyValueText(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return notiontext.Plain(v)
	}
	typ, _ := m["type"].(string)
	if typ == "" {
		return notiontext.Plain(v)
	}
	switch typ {
	case "title", "rich_text":
		return notiontext.Plain(m[typ])
	case "number":
		return numberText(m["number"])
	case "select", "status":
		return namedObject(m[typ])
	case "multi_select":
		return joinNamed(m[typ])
	case "date":
		return dateText(m["date"])
	case "checkbox":
		if b, ok := m["checkbox"].(bool); ok {
			return strconv.FormatBool(b)
		}
	case "url", "email", "phone_number", "created_time", "last_edited_time":
		if s, ok := m[typ].(string); ok {
			return s
		}
	case "people", "files":
		return joinNamed(m[typ])
	case "relation":
		return joinIDs(m[typ])
	case "formula":
		return formulaText(m["formula"])
	case "rollup":
		return rollupText(m["rollup"])
	case "created_by", "last_edited_by":
		return namedObject(m[typ])
	case "unique_id":
		return uniqueIDText(m["unique_id"])
	}
	return notiontext.Plain(v)
}

func namedObject(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	if name, ok := m["name"].(string); ok {
		return name
	}
	if id, ok := m["id"].(string); ok {
		return id
	}
	return notiontext.Plain(v)
}

func joinNamed(v any) string {
	items, ok := v.([]any)
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		if text := namedObject(item); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, ", ")
}

func joinIDs(v any) string {
	items, ok := v.([]any)
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if id, ok := m["id"].(string); ok {
			parts = append(parts, id)
		}
	}
	return strings.Join(parts, ", ")
}

func dateText(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	start, _ := m["start"].(string)
	end, _ := m["end"].(string)
	if end != "" {
		return start + "/" + end
	}
	return start
}

func formulaText(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	typ, _ := m["type"].(string)
	switch typ {
	case "string":
		s, _ := m["string"].(string)
		return s
	case "number":
		return numberText(m["number"])
	case "boolean":
		if b, ok := m["boolean"].(bool); ok {
			return strconv.FormatBool(b)
		}
	case "date":
		return dateText(m["date"])
	}
	return notiontext.Plain(v)
}

func rollupText(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	typ, _ := m["type"].(string)
	switch typ {
	case "number":
		return numberText(m["number"])
	case "date":
		return dateText(m["date"])
	case "array":
		items, _ := m["array"].([]any)
		parts := make([]string, 0, len(items))
		for _, item := range items {
			if text := propertyValueText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, ", ")
	}
	return notiontext.Plain(v)
}

func uniqueIDText(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	prefix, _ := m["prefix"].(string)
	number := numberText(m["number"])
	return prefix + number
}

func numberText(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case json.Number:
		return x.String()
	default:
		return fmt.Sprint(x)
	}
}
