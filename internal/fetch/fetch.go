// Package fetch pulls usage and cost reports from a provider's admin API and
// saves every page as a JSON file. It deliberately does no parsing: what
// `analyze` reads is the file on disk, which the user can open and inspect.
// Keys come from the caller and are only ever sent to that provider's API.
package fetch

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Base URLs, variables so tests can point them at a local server.
var (
	OpenAIBase    = "https://api.openai.com/v1"
	AnthropicBase = "https://api.anthropic.com/v1"
)

// MaxPages bounds a runaway pagination loop.
const MaxPages = 200

var client = &http.Client{Timeout: 60 * time.Second}

func window(since, until string) (time.Time, time.Time, error) {
	start, err := time.Parse("2006-01-02", since)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("--since %q: want YYYY-MM-DD", since)
	}
	end := time.Now().UTC().Truncate(24 * time.Hour)
	if until != "" {
		if end, err = time.Parse("2006-01-02", until); err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("--until %q: want YYYY-MM-DD", until)
		}
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("--until must be after --since")
	}
	return start.UTC(), end.UTC(), nil
}

// getPages follows next-page cursors and saves each body to outDir with the
// given prefix. pageParam names the cursor query parameter; nextOf extracts
// the cursor from a body, returning "" when there is none.
func getPages(base, path string, query url.Values, headers map[string]string, outDir, prefix string, nextOf func([]byte) string) ([]string, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	var files []string
	cursor := ""
	for n := 1; n <= MaxPages; n++ {
		q := url.Values{}
		for k, v := range query {
			q[k] = v
		}
		if cursor != "" {
			q.Set("page", cursor)
		}
		req, err := http.NewRequest(http.MethodGet, base+path+"?"+q.Encode(), nil)
		if err != nil {
			return files, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			return files, fmt.Errorf("%s: %w", path, err)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if err != nil {
			return files, fmt.Errorf("%s: reading response: %w", path, err)
		}
		if resp.StatusCode/100 != 2 {
			snippet := strings.TrimSpace(string(body))
			if len(snippet) > 300 {
				snippet = snippet[:300] + "…"
			}
			return files, fmt.Errorf("%s: HTTP %d: %s%s", path, resp.StatusCode, snippet, permissionHint(base, resp.StatusCode))
		}
		name := filepath.Join(outDir, fmt.Sprintf("%s-%03d.json", prefix, n))
		if err := os.WriteFile(name, body, 0o600); err != nil {
			return files, err
		}
		files = append(files, name)
		cursor = nextOf(body)
		if cursor == "" {
			return files, nil
		}
	}
	return files, fmt.Errorf("%s: more than %d pages; narrow the window", path, MaxPages)
}

// permissionHint says, in plain words, what a 401 or 403 from a provider's
// report endpoint almost always means: the wrong kind of key. Both providers
// gate these endpoints behind organization admin keys that ordinary project
// keys cannot stand in for.
func permissionHint(base string, status int) string {
	if status != http.StatusUnauthorized && status != http.StatusForbidden {
		return ""
	}
	// Decided by the host, not by the package variables: tests point those at
	// a local server and must not inherit a provider's advice.
	switch {
	case strings.Contains(base, "openai"):
		return "\n\n  OpenAI's usage and costs endpoints need an Admin API key, not a project key:\n" +
			"  platform.openai.com → Settings → Organization → Admin keys (requires the Owner role).\n" +
			"  Admin keys start with sk-admin-. A restricted key must carry the api.usage.read scope.\n" +
			"  Put it in OPENAI_ADMIN_KEY and run fetch again."
	case strings.Contains(base, "anthropic"):
		return "\n\n  Anthropic's usage and cost reports are Admin API endpoints. \"requires an Admin API key or an\n" +
			"  organization-scoped API key\" means the key is scoped to a single workspace; a key created with\n" +
			"  Scope: Default workspace cannot reach these endpoints. Make one that spans the organization:\n" +
			"    platform.claude.com -> Settings -> API keys -> Create key, and set Scope to Organization\n" +
			"    (not a workspace). The key still starts sk-ant-api...; that is fine, only the scope matters.\n" +
			"    (If your Console shows a separate Admin keys page, a key made there works too.)\n" +
			"  Put it in ANTHROPIC_ADMIN_KEY and run fetch again. \"API key is invalid\" instead means the value is\n" +
			"  not a live key (expired, disabled, or mistyped); the API keys page shows each key's status and expiry.\n" +
			"  The Admin API is unavailable to individual accounts without an organization."
	}
	return ""
}

// KeyShapeWarning returns a warning when a key does not look like the kind
// the provider's report endpoints accept, or "" when it does. Prefixes are a
// convention, not a contract, so this warns and never refuses.
func KeyShapeWarning(provider, key string) string {
	switch provider {
	case "openai":
		if strings.HasSuffix(key, "...") || len(key) < 40 {
			return "OPENAI_ADMIN_KEY looks like a placeholder or a truncated key; paste the whole key. Trying anyway."
		}
		if strings.TrimSpace(key) != key {
			return "OPENAI_ADMIN_KEY has leading or trailing whitespace; it will be trimmed."
		}
		if !strings.HasPrefix(key, "sk-admin-") {
			return "OPENAI_ADMIN_KEY does not start with sk-admin-: OpenAI's usage and costs endpoints need an Admin API key (Settings → Organization → Admin keys), not a project key. Trying anyway."
		}
	case "anthropic":
		if strings.HasSuffix(key, "...") || len(key) < 40 {
			return "ANTHROPIC_ADMIN_KEY looks like a placeholder or a truncated key; paste the whole key. Trying anyway."
		}
		if strings.TrimSpace(key) != key {
			return "ANTHROPIC_ADMIN_KEY has leading or trailing whitespace; it will be trimmed."
		}
		if !strings.HasPrefix(key, "sk-ant-") {
			return "ANTHROPIC_ADMIN_KEY does not start with sk-ant-: create it on the API keys page with Scope set to Organization (or an sk-ant-admin… Admin key). Trying anyway."
		}
	}
	return ""
}

// nextPage reads a "next_page" string out of a JSON body without a full
// parse: both providers put it at the top level, next to has_more.
func nextPage(body []byte) string {
	s := string(body)
	i := strings.LastIndex(s, `"next_page"`)
	if i < 0 {
		return ""
	}
	rest := s[i+len(`"next_page"`):]
	rest = strings.TrimLeft(rest, " :\n\t")
	if !strings.HasPrefix(rest, `"`) {
		return "" // null
	}
	rest = rest[1:]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// isUnauthorized reports whether an error from getPages was a 401.
func isUnauthorized(err error) bool {
	return err != nil && strings.Contains(err.Error(), "HTTP 401")
}

// firstLine keeps an error's first line: the retry's own hint would repeat it.
func firstLine(err error) string {
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}

// OpenAI saves the completions usage pages and the costs pages for the window.
// Usage is grouped by project, key and model; costs by project and line item.
func OpenAI(adminKey, since, until, outDir string) ([]string, error) {
	start, end, err := window(since, until)
	if err != nil {
		return nil, err
	}
	adminKey = strings.TrimSpace(adminKey)
	headers := map[string]string{"Authorization": "Bearer " + adminKey}
	usage := url.Values{
		"start_time":   {strconv.FormatInt(start.Unix(), 10)},
		"end_time":     {strconv.FormatInt(end.Unix(), 10)},
		"bucket_width": {"1d"},
		"limit":        {"31"},
		"group_by":     {"project_id", "api_key_id", "model"},
	}
	files, err := getPages(OpenAIBase, "/organization/usage/completions", usage, headers, outDir, "openai-usage", nextPage)
	if err != nil {
		return files, err
	}
	costs := url.Values{
		"start_time":   {strconv.FormatInt(start.Unix(), 10)},
		"end_time":     {strconv.FormatInt(end.Unix(), 10)},
		"bucket_width": {"1d"},
		"limit":        {"31"},
		"group_by":     {"project_id", "line_item"},
	}
	more, err := getPages(OpenAIBase, "/organization/costs", costs, headers, outDir, "openai-costs", nextPage)
	return append(files, more...), err
}

// Anthropic saves the messages usage report and the cost report for the
// window. Usage is grouped by key, workspace and model; cost by workspace and
// description. These are Admin API endpoints: they do not accept a workspace
// header (a workspace is a group_by/filter query parameter, not a header), so
// none is sent.
func Anthropic(adminKey, since, until, outDir string) ([]string, error) {
	start, end, err := window(since, until)
	if err != nil {
		return nil, err
	}
	adminKey = strings.TrimSpace(adminKey)
	headers := map[string]string{"x-api-key": adminKey, "anthropic-version": "2023-06-01"}
	usage := url.Values{
		"starting_at":  {start.Format(time.RFC3339)},
		"ending_at":    {end.Format(time.RFC3339)},
		"bucket_width": {"1d"},
		"limit":        {"31"},
		"group_by[]":   {"api_key_id", "workspace_id", "model"},
	}
	files, err := getPages(AnthropicBase, "/organizations/usage_report/messages", usage, headers, outDir, "anthropic-usage", nextPage)
	if isUnauthorized(err) {
		// The Admin API also accepts an OAuth-style bearer credential; a key
		// that the x-api-key path rejects may be accepted there.
		headers = map[string]string{"Authorization": "Bearer " + adminKey, "anthropic-version": "2023-06-01"}
		var retryErr error
		files, retryErr = getPages(AnthropicBase, "/organizations/usage_report/messages", usage, headers, outDir, "anthropic-usage", nextPage)
		if retryErr != nil {
			return files, fmt.Errorf("%w\n  (also tried as a bearer token: %v)", err, firstLine(retryErr))
		}
		err = nil
	}
	if err != nil {
		return files, err
	}
	cost := url.Values{
		"starting_at":  {start.Format(time.RFC3339)},
		"ending_at":    {end.Format(time.RFC3339)},
		"bucket_width": {"1d"},
		"limit":        {"31"},
		"group_by[]":   {"workspace_id", "description"},
	}
	more, err := getPages(AnthropicBase, "/organizations/cost_report", cost, headers, outDir, "anthropic-costs", nextPage)
	return append(files, more...), err
}
