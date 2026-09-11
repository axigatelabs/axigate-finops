package gateway

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestKeyFingerprintNeverKeepsTheKey(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer sk-proj-verysecretkey1234")
	fp := keyFingerprint("openai", h, nil)
	if !strings.HasPrefix(fp, "k-") || !strings.HasSuffix(fp, "-1234") || strings.Contains(fp, "secret") || len(fp) != 2+12+1+4 {
		t.Fatalf("fingerprint should be k-<12 hex>-<last 4>, got %q", fp)
	}
	if keyLabel(fp) != "key …1234" {
		t.Fatalf("label = %q", keyLabel(fp))
	}
	// The same key gives the same fingerprint whichever header carries it.
	h2 := http.Header{}
	h2.Set("x-api-key", "sk-proj-verysecretkey1234")
	if keyFingerprint("anthropic", h2, nil) != fp {
		t.Fatal("x-api-key and Bearer must fingerprint the same key alike")
	}
	h3 := http.Header{}
	h3.Set("x-goog-api-key", "sk-proj-verysecretkey1234")
	if keyFingerprint("gemini", h3, nil) != fp {
		t.Fatal("x-goog-api-key must fingerprint alike")
	}
	if keyFingerprint("gemini", http.Header{}, url.Values{"key": {"sk-proj-verysecretkey1234"}}) != fp {
		t.Fatal("a ?key= query must fingerprint alike")
	}
	// A different key is a different fingerprint even with the same last four.
	h4 := http.Header{}
	h4.Set("Authorization", "Bearer sk-proj-othersecretkey1234")
	if keyFingerprint("openai", h4, nil) == fp {
		t.Fatal("different keys must not collide")
	}
	// X-AxiGate-Key is a label: the credential keeps its one fingerprint under
	// any name, so whoever holds the key cannot dodge its ceiling by renaming.
	h.Set("X-AxiGate-Key", "ci-runner")
	if keyFingerprint("openai", h, nil) != fp {
		t.Fatal("a label must not replace the credential's identity")
	}
	if keyName(h) != "ci-runner" || keyLabel("ci-runner") != "key ci-runner" {
		t.Fatalf("the name is kept for display: %q", keyName(h))
	}
	// A keyless caller can still be capped under the name it chose.
	named := http.Header{}
	named.Set("X-AxiGate-Key", "batch-job")
	if keyFingerprint("openai", named, nil) != "batch-job" {
		t.Fatal("with no credential the name stands in")
	}
	named.Set("X-AxiGate-Key", strings.Repeat("n", 100))
	if len(keyFingerprint("openai", named, nil)) != maxKeyName {
		t.Fatal("a name is bounded")
	}
	named.Set("X-AxiGate-Key", strings.Repeat("é", 100))
	if got := keyFingerprint("openai", named, nil); !utf8.ValidString(got) || utf8.RuneCountInString(got) != maxKeyName {
		t.Fatalf("a name is cut on a character boundary: %q", got)
	}
	named.Set("X-AxiGate-Key", fp)
	if keyFingerprint("openai", named, nil) != "" {
		t.Fatal("a bare name shaped like a fingerprint cannot stand in for a real key")
	}
	if keyFingerprint("openai", http.Header{}, nil) != "" {
		t.Fatal("no credential and no name means no key")
	}
}

func TestKeyFingerprintHidesAShortToken(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer abc")
	fp := keyFingerprint("openai", h, nil)
	if strings.Contains(fp, "abc") || !strings.HasSuffix(fp, "-") || len(fp) != 2+12+1 {
		t.Fatalf("a short token gets no visible suffix, got %q", fp)
	}
	if keyLabel(fp) != "key "+fp[2:14] {
		t.Fatalf("label of a suffix-less fingerprint = %q", keyLabel(fp))
	}
	h.Set("Authorization", "Bearer 12345678")
	if fp := keyFingerprint("openai", h, nil); !strings.HasSuffix(fp, "-5678") {
		t.Fatalf("eight characters is long enough for a suffix: %q", fp)
	}
	// Only the exact fingerprint shape is shortened; a name is shown as given.
	if keyLabel("k-ci-runner") != "key k-ci-runner" {
		t.Fatalf("a name that starts with k- is still a name: %q", keyLabel("k-ci-runner"))
	}
}

// The identity is the credential the provider pays against, so a stray
// header next to the real one cannot mint a fresh identity per call.
func TestKeyFingerprintReadsTheProvidersOwnHeaderFirst(t *testing.T) {
	a := http.Header{}
	a.Set("X-Api-Key", "sk-ant-realkey-4444")
	real := keyFingerprint("anthropic", a, nil)
	a.Set("Authorization", "Bearer junk-1")
	if keyFingerprint("anthropic", a, nil) != real {
		t.Fatal("anthropic: x-api-key is the credential, not a stray Authorization")
	}
	g := http.Header{}
	g.Set("Authorization", "Bearer junk-2")
	q := url.Values{"key": {"AIza-real-key-5555"}}
	if keyFingerprint("gemini", g, q) != keyFingerprint("gemini", http.Header{}, q) {
		t.Fatal("gemini: ?key= is the credential, not a stray Authorization")
	}
	o := http.Header{}
	o.Set("Authorization", "Bearer sk-real-6666")
	want := keyFingerprint("openai", o, nil)
	o.Set("X-Api-Key", "junk-3")
	if keyFingerprint("openai", o, nil) != want {
		t.Fatal("openai: the bearer token is the credential")
	}
}
