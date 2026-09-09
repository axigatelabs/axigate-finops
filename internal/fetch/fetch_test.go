package fetch

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNextPage(t *testing.T) {
	cases := map[string]string{
		`{"data":[],"has_more":true,"next_page":"page_AAA"}`: "page_AAA",
		`{"data":[],"has_more":false,"next_page":null}`:      "",
		`{"data":[]}`: "",
		`{"data":[{"next_page":"inner"}],"has_more":true,"next_page": "outer"}`: "outer",
	}
	for body, want := range cases {
		if got := nextPage([]byte(body)); got != want {
			t.Errorf("%s: got %q want %q", body, got, want)
		}
	}
}

func TestOpenAIFollowsCursorsAndSavesPages(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path+"?"+r.URL.RawQuery)
		if r.Header.Get("Authorization") != "Bearer test-admin" {
			http.Error(w, "no auth", http.StatusUnauthorized)
			return
		}
		switch {
		case r.URL.Path == "/organization/usage/completions" && r.URL.Query().Get("page") == "":
			w.Write([]byte(`{"object":"page","data":[],"has_more":true,"next_page":"page_2"}`))
		case r.URL.Path == "/organization/usage/completions":
			w.Write([]byte(`{"object":"page","data":[],"has_more":false,"next_page":null}`))
		case r.URL.Path == "/organization/costs":
			w.Write([]byte(`{"object":"page","data":[],"has_more":false,"next_page":null}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	old := OpenAIBase
	OpenAIBase = srv.URL
	t.Cleanup(func() { OpenAIBase = old })
	out := t.TempDir()
	files, err := OpenAI("test-admin", "2025-08-01", "2025-09-01", out)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 {
		t.Fatalf("files: %v", files)
	}
	for _, f := range files {
		if _, err := os.Stat(f); err != nil {
			t.Fatal(err)
		}
	}
	if filepath.Base(files[1]) != "openai-usage-002.json" || filepath.Base(files[2]) != "openai-costs-001.json" {
		t.Fatalf("names: %v", files)
	}
	if len(seen) != 3 || !strings.Contains(seen[1], "page=page_2") {
		t.Fatalf("requests: %v", seen)
	}
	q := seen[0]
	for _, want := range []string{"bucket_width=1d", "group_by=project_id", "group_by=api_key_id", "group_by=model", "start_time=1754006400", "end_time=1756684800"} {
		if !strings.Contains(q, want) {
			t.Fatalf("usage query missing %s: %s", want, q)
		}
	}
	if !strings.Contains(seen[2], "group_by=line_item") {
		t.Fatalf("costs query: %s", seen[2])
	}
}

func TestAnthropicHeadersAndArrayParams(t *testing.T) {
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path+"?"+r.URL.RawQuery)
		if r.Header.Get("x-api-key") != "adm" || r.Header.Get("anthropic-version") == "" {
			http.Error(w, "no auth", http.StatusUnauthorized)
			return
		}
		if ws := r.Header.Get("anthropic-workspace-id"); ws != "" {
			http.Error(w, "the Admin API rejects a workspace header: "+ws, http.StatusBadRequest)
			return
		}
		w.Write([]byte(`{"data":[],"has_more":false,"next_page":null}`))
	}))
	defer srv.Close()
	old := AnthropicBase
	AnthropicBase = srv.URL
	t.Cleanup(func() { AnthropicBase = old })
	files, err := Anthropic("adm", "2025-08-01", "", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || len(seen) != 2 {
		t.Fatalf("files %v requests %v", files, seen)
	}
	if !strings.Contains(seen[0], "group_by%5B%5D=api_key_id") || !strings.Contains(seen[0], "starting_at=2025-08-01T00%3A00%3A00Z") {
		t.Fatalf("usage query: %s", seen[0])
	}
	if !strings.Contains(seen[1], "/organizations/cost_report") || !strings.Contains(seen[1], "group_by%5B%5D=description") {
		t.Fatalf("cost query: %s", seen[1])
	}
}

func TestPermissionHintsNameTheRightKindOfKey(t *testing.T) {
	if h := permissionHint("https://api.openai.com/v1", http.StatusForbidden); !strings.Contains(h, "Admin API key") || !strings.Contains(h, "api.usage.read") {
		t.Fatalf("openai 403 hint: %q", h)
	}
	if h := permissionHint("https://api.anthropic.com/v1", http.StatusUnauthorized); !strings.Contains(h, "Scope to Organization") || !strings.Contains(h, "single workspace") || !strings.Contains(h, "individual accounts") || !strings.Contains(h, "expired") {
		t.Fatalf("anthropic 401 hint: %q", h)
	}
	if h := permissionHint("https://api.openai.com/v1", http.StatusInternalServerError); h != "" {
		t.Fatalf("a 500 is not a permissions problem: %q", h)
	}
	if h := permissionHint("http://127.0.0.1:1", http.StatusForbidden); h != "" {
		t.Fatalf("a test server gets no provider hint: %q", h)
	}
	if w := KeyShapeWarning("openai", "sk-proj-"+strings.Repeat("b", 48)); !strings.Contains(w, "sk-admin-") {
		t.Fatalf("project key warning: %q", w)
	}
	if w := KeyShapeWarning("openai", "sk-admin-"+strings.Repeat("a", 48)); w != "" {
		t.Fatalf("admin key must not warn: %q", w)
	}
	// A personal key created with Scope: Organization is a legitimate Admin API credential.
	if w := KeyShapeWarning("anthropic", "sk-ant-api03-"+strings.Repeat("c", 48)); w != "" {
		t.Fatalf("an organization-scoped personal key must not warn: %q", w)
	}
	if w := KeyShapeWarning("anthropic", " sk-ant-admin01-"+strings.Repeat("c", 48)+"\n"); !strings.Contains(w, "whitespace") {
		t.Fatalf("whitespace must be named: %q", w)
	}
	if w := KeyShapeWarning("anthropic", "abc-"+strings.Repeat("c", 48)); !strings.Contains(w, "sk-ant-") {
		t.Fatal("a regular anthropic key must warn")
	}
	if w := KeyShapeWarning("anthropic", "sk-ant-admin01-"+strings.Repeat("x", 40)); w != "" {
		t.Fatalf("anthropic admin key must not warn: %q", w)
	}
	if w := KeyShapeWarning("anthropic", "sk-ant-admin..."); !strings.Contains(w, "placeholder") {
		t.Fatalf("a pasted placeholder must be named: %q", w)
	}
	if w := KeyShapeWarning("openai", "sk-admin-..."); !strings.Contains(w, "placeholder") {
		t.Fatalf("a pasted placeholder must be named: %q", w)
	}
}

func TestErrorsAreNamedAndNeverLeakTheKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"insufficient permissions"}`, http.StatusForbidden)
	}))
	defer srv.Close()
	old := OpenAIBase
	OpenAIBase = srv.URL
	t.Cleanup(func() { OpenAIBase = old })
	_, err := OpenAI("sk-admin-secret", "2025-08-01", "2025-08-02", t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "HTTP 403") || strings.Contains(err.Error(), "sk-admin-secret") {
		t.Fatalf("error: %v", err)
	}
	if _, err := OpenAI("k", "August", "", t.TempDir()); err == nil {
		t.Fatal("bad date accepted")
	}
	if _, err := OpenAI("k", "2025-08-02", "2025-08-01", t.TempDir()); err == nil {
		t.Fatal("inverted window accepted")
	}
}

func TestAnthropicFallsBackToBearerOn401(t *testing.T) {
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("x-api-key")+"|"+r.Header.Get("Authorization"))
		if r.Header.Get("Authorization") != "Bearer adm" {
			http.Error(w, `{"type":"error","error":{"type":"authentication_error","message":"API key is invalid."}}`, http.StatusUnauthorized)
			return
		}
		w.Write([]byte(`{"data":[],"has_more":false,"next_page":null}`))
	}))
	defer srv.Close()
	old := AnthropicBase
	AnthropicBase = srv.URL
	t.Cleanup(func() { AnthropicBase = old })
	files, err := Anthropic(" adm\n", "2025-08-01", "2025-08-02", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files: %v", files)
	}
	// first attempt x-api-key (rejected), then bearer for the usage report, then bearer for the cost report
	if len(auths) != 3 || auths[0] != "adm|" || auths[1] != "|Bearer adm" || auths[2] != "|Bearer adm" {
		t.Fatalf("auth attempts: %v", auths)
	}
}

func TestAnthropicReportsBothFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"type":"error","error":{"type":"authentication_error","message":"API key is invalid."}}`, http.StatusUnauthorized)
	}))
	defer srv.Close()
	old := AnthropicBase
	AnthropicBase = srv.URL
	t.Cleanup(func() { AnthropicBase = old })
	_, err := Anthropic("nope", "2025-08-01", "2025-08-02", t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "also tried as a bearer token") {
		t.Fatalf("error: %v", err)
	}
}
