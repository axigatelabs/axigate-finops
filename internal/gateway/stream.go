package gateway

import (
	"bytes"
	"encoding/json"

	"github.com/axigatelabs/axigate-finops/internal/ledger"
)

// sseUsage reads token usage out of a Server-Sent Events stream as it passes,
// without changing a byte the client receives. It is an io.Writer used as the
// tee sink for a streamed response: the gateway copies the upstream body to the
// client and to this at once. Usage rides the final events, so it accumulates
// across frames:
//
//   - OpenAI streams the full usage on a late chunk (only when the caller set
//     stream_options.include_usage); we take the last usage seen.
//   - Anthropic puts input and cache on message_start and the final output on
//     message_delta; we take input/cache from the first and output from the
//     last.
//
// A stream that never carries usage (OpenAI without include_usage) leaves
// gotUsage false, so the event is honestly marked usage-unknown rather than
// priced from nothing.
type sseUsage struct {
	provider string
	limit    int64
	buf      []byte
	usage    ledger.Usage
	model    string
	got      bool
}

func newSSEUsage(provider string, limit int64) *sseUsage {
	if limit <= 0 {
		limit = defaultMaxBody
	}
	return &sseUsage{provider: provider, limit: limit}
}

func (s *sseUsage) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	for {
		i := bytes.IndexByte(s.buf, '\n')
		if i < 0 {
			break
		}
		line := s.buf[:i]
		s.buf = s.buf[i+1:]
		s.consume(line)
	}
	// A pathological stream with no newlines must not grow memory without bound;
	// beyond the cap we drop the unterminated remainder (it cannot be a frame we
	// can parse anyway).
	if int64(len(s.buf)) > s.limit {
		s.buf = s.buf[:0]
	}
	return len(p), nil
}

// sseFrame unions the usage-bearing shapes of both providers' streamed events.
type sseFrame struct {
	Model   string `json:"model"`
	Message struct {
		Model string   `json:"model"`
		Usage rawUsage `json:"usage"`
	} `json:"message"`
	Usage rawUsage `json:"usage"`
}

type rawUsage struct {
	PromptTokens        int64 `json:"prompt_tokens"`
	CompletionTokens    int64 `json:"completion_tokens"`
	PromptTokensDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	InputTokens           int64 `json:"input_tokens"`
	OutputTokens          int64 `json:"output_tokens"`
	CacheReadInputTokens  int64 `json:"cache_read_input_tokens"`
	CacheCreationInputTok int64 `json:"cache_creation_input_tokens"`
	CacheCreation         struct {
		Ephemeral5m int64 `json:"ephemeral_5m_input_tokens"`
		Ephemeral1h int64 `json:"ephemeral_1h_input_tokens"`
	} `json:"cache_creation"`
}

func (s *sseUsage) consume(line []byte) {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	data := bytes.TrimSpace(line[len("data:"):])
	if len(data) == 0 || data[0] != '{' {
		return // "[DONE]" and event: lines carry no usage
	}
	var f sseFrame
	if json.Unmarshal(data, &f) != nil {
		return
	}
	if f.Model != "" {
		s.model = f.Model
	} else if f.Message.Model != "" {
		s.model = f.Message.Model
	}
	switch s.provider {
	case "openai":
		u := f.Usage
		if u.PromptTokens > 0 || u.CompletionTokens > 0 { // the final chunk carries the totals
			s.usage.InputTokens = u.PromptTokens
			s.usage.CacheRead = u.PromptTokensDetails.CachedTokens
			s.usage.OutputTokens = u.CompletionTokens
			s.got = true
		}
	case "anthropic":
		// input and cache arrive on message_start (nested under message.usage);
		// output arrives on message_delta (top-level usage). Merge both.
		start := f.Message.Usage
		if start.InputTokens > 0 || start.CacheReadInputTokens > 0 || start.CacheCreationInputTok > 0 {
			s.usage.InputTokens = start.InputTokens
			s.usage.CacheRead = start.CacheReadInputTokens
			w5, w1 := start.CacheCreation.Ephemeral5m, start.CacheCreation.Ephemeral1h
			if w5 == 0 && w1 == 0 {
				w5 = start.CacheCreationInputTok
			}
			s.usage.CacheWrite5m, s.usage.CacheWrite1h = w5, w1
			s.got = true
		}
		if f.Usage.OutputTokens > 0 {
			s.usage.OutputTokens = f.Usage.OutputTokens
			s.got = true
		}
	}
}

// result returns the accumulated usage, the model if the stream named one, and
// whether any usage was seen at all.
func (s *sseUsage) result() (ledger.Usage, string, bool) {
	return s.usage, s.model, s.got
}
