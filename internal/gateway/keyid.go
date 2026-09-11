package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"
)

// A per-key ceiling needs a name for the key that is not the key. The gateway
// never records a credential: it reads the one the request carries, keeps a
// short fingerprint of it, and shows the last four characters the way
// providers' own consoles do — only when the credential is long enough that
// four characters give nothing away. A caller may label the key with
// X-AxiGate-Key ("ci-runner"); the label is shown, but the ceiling binds to
// the credential, so whoever holds the key cannot dodge it by renaming.
//
// Fingerprints look like k-3fa9c2b1e0d4-abcd: twelve hex characters of the
// key's SHA-256, then its last four characters (or nothing after the dash for
// a short token). A real provider key cannot be recovered from that; a short
// or guessable token (a self-hosted upstream's --api-key) could be, by
// guessing, so the fingerprint is not a secret-safe name for those.

const (
	minKeyLenForSuffix = 8
	maxKeyName         = 64
)

var fingerprintShape = regexp.MustCompile(`^k-[0-9a-f]{12}-`)

// keyFingerprint reads the credential the provider will pay against and
// returns its fingerprint: the provider's own header first, so a stray
// Authorization header alongside a real x-api-key cannot stand in for it.
// With no credential it returns the caller's X-AxiGate-Key label, if any, so
// a keyless caller can still be capped under a name it chose; with neither it
// returns "" (such a call has no key ceiling, the way an untagged call has no
// run cap).
func keyFingerprint(provider string, h http.Header, q url.Values) string {
	bearer := ""
	if v := h.Get("Authorization"); len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
		bearer = strings.TrimSpace(v[7:])
	}
	var order []string
	switch provider {
	case "anthropic":
		order = []string{h.Get("X-Api-Key"), bearer}
	case "gemini":
		order = []string{h.Get("X-Goog-Api-Key"), q.Get("key"), bearer}
	default: // openai and every OpenAI-compatible upstream
		order = []string{bearer, h.Get("X-Api-Key")}
	}
	raw := ""
	for _, c := range order {
		if raw == "" {
			raw = strings.TrimSpace(c)
		}
	}
	if raw == "" {
		if name := keyName(h); !fingerprintShape.MatchString(name) {
			return name
		}
		return "" // a name shaped like a fingerprint could spend against a real key's tally
	}
	sum := sha256.Sum256([]byte(raw))
	suffix := ""
	if len(raw) >= minKeyLenForSuffix {
		suffix = raw[len(raw)-4:]
	}
	return "k-" + hex.EncodeToString(sum[:6]) + "-" + suffix
}

// keyName is the label a caller gave with X-AxiGate-Key, trimmed and bounded
// to maxKeyName characters (whole characters, so a label stays valid text).
func keyName(h http.Header) string {
	name := strings.TrimSpace(h.Get("X-AxiGate-Key"))
	if utf8.RuneCountInString(name) > maxKeyName {
		rs := []rune(name)
		name = string(rs[:maxKeyName])
	}
	return name
}

// keyLabel is how a key is named in a sentence: "key …abcd" for a
// fingerprint with a suffix, "key 3fa9c2b1e0d4" for one without, "key
// ci-runner" for a name.
func keyLabel(key string) string {
	if fingerprintShape.MatchString(key) {
		if suffix := key[len("k-")+12+1:]; suffix != "" {
			return "key …" + suffix
		}
		return "key " + key[len("k-"):len("k-")+12]
	}
	return "key " + key
}
