package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/state"
	"github.com/mattsp1290/eino-channels/internal/testkit"
)

// providerRequest is one captured OpenCode Go request.
type providerRequest struct {
	Path, UserAgent, Session, Auth string
	Body                           map[string]any
}

// fakeOpenCode is a scripted OpenCode Go Chat Completions server.
type fakeOpenCode struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []providerRequest
	// Mode per request index: "" (echo), "401", "429", "truncate", "slow".
	modes []string
	// Delay between deltas to make previews observable.
	delay time.Duration
}

func newFakeOpenCode(t *testing.T) *fakeOpenCode {
	t.Helper()
	f := &fakeOpenCode{}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeOpenCode) push(mode string) {
	f.mu.Lock()
	f.modes = append(f.modes, mode)
	f.mu.Unlock()
}

func (f *fakeOpenCode) requests() []providerRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]providerRequest(nil), f.reqs...)
}

func (f *fakeOpenCode) handle(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	f.mu.Lock()
	f.reqs = append(f.reqs, providerRequest{Path: r.URL.Path, UserAgent: r.Header.Get("User-Agent"), Session: r.Header.Get("x-opencode-session"), Auth: r.Header.Get("Authorization"), Body: body})
	mode := ""
	if len(f.modes) != 0 {
		mode = f.modes[0]
		f.modes = f.modes[1:]
	}
	delay := f.delay
	f.mu.Unlock()
	if r.URL.Path != "/zen/go/v1/chat/completions" {
		http.Error(w, `{"error":{"message":"not found"}}`, 404)
		return
	}
	switch mode {
	case "401":
		w.WriteHeader(401)
		_, _ = io.WriteString(w, `{"error":{"message":"SENTINEL_PROVIDER_BODY invalid key","type":"authentication_error"}}`)
		return
	case "429":
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
		_, _ = io.WriteString(w, `{"error":{"message":"SENTINEL_PROVIDER_BODY slow down","type":"rate_limit_error"}}`)
		return
	case "slow":
		time.Sleep(3 * time.Second)
		w.WriteHeader(500)
		return
	}
	// Echo the last user message content.
	last := ""
	if msgs, ok := body["messages"].([]any); ok {
		for i := len(msgs) - 1; i >= 0; i-- {
			m, _ := msgs[i].(map[string]any)
			if m["role"] == "user" {
				last, _ = m["content"].(string)
				break
			}
		}
	}
	reply := "reply:" + last
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	flusher, _ := w.(http.Flusher)
	chunk := func(content string, finish bool) {
		delta := map[string]any{"content": content}
		choice := map[string]any{"index": 0, "delta": delta}
		if finish {
			choice["finish_reason"] = "stop"
		}
		payload, _ := json.Marshal(map[string]any{"id": "id", "object": "chat.completion.chunk", "created": 0, "model": "deepseek-v4-flash", "choices": []any{choice}})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
		if flusher != nil {
			flusher.Flush()
		}
	}
	if mode == "truncate" {
		chunk("partial", false)
		return // connection closes without [DONE]
	}
	half := len(reply) / 2
	chunk(reply[:half], false)
	if delay > 0 {
		time.Sleep(delay)
	}
	chunk(reply[half:], true)
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
}

// realEnv assembles state, a real provider bridge against the fake server,
// and the service with a recording deliverer per platform.
type realEnv struct {
	t         testing.TB
	dir       string
	store     *state.Store
	bridge    *agentbridge.Bridge
	svc       *conversation.Service
	slack     *testkit.Deliverer
	discord   *testkit.Deliverer
	logs      *testkit.LogBuffer
	cancel    context.CancelFunc
	limits    config.Limits
	provider  *fakeOpenCode
	apiKey    string
	userAgent string
}

func openReal(t testing.TB, dir string, provider *fakeOpenCode, limits config.Limits) *realEnv {
	t.Helper()
	ctx := context.Background()
	st, err := state.Open(ctx, dir, state.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	e := &realEnv{t: t, dir: dir, store: st, provider: provider, limits: limits, apiKey: "SENTINEL_API_KEY", userAgent: "eino-channels/test"}
	e.bridge, err = agentbridge.New(ctx, agentbridge.Options{Store: st.Agent(), OwnerID: fmt.Sprintf("it-%d", time.Now().UnixNano()), APIKey: e.apiKey, UserAgent: e.userAgent, BaseURL: provider.srv.URL + "/zen/go/v1"})
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}
	e.slack, e.discord = testkit.NewDeliverer(3500), testkit.NewDeliverer(1800)
	e.logs = &testkit.LogBuffer{}
	e.svc, err = conversation.New(conversation.Options{Limits: limits, Store: st, Bridge: e.bridge, Deliverers: map[state.Platform]conversation.Deliverer{state.PlatformSlack: e.slack, state.PlatformDiscord: e.discord}, Logger: slog.New(slog.NewTextHandler(e.logs, nil)), DeliverySpacing: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("service: %v", err)
	}
	rctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.svc.Start(rctx)
	if tt, ok := t.(*testing.T); ok {
		tt.Cleanup(func() {
			if tt.Failed() {
				tt.Logf("service logs:\n%s", e.logs.String())
			}
		})
	}
	return e
}

func (e *realEnv) close() {
	if e.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	_ = e.svc.Shutdown(ctx)
	cancel()
	e.cancel()
	_ = e.bridge.Close(context.Background())
	_ = e.store.Close()
	e.store = nil
}

func (e *realEnv) ingest(in state.Inbound) conversation.Response {
	e.t.Helper()
	resp, err := e.svc.Ingest(context.Background(), in)
	if err != nil {
		e.t.Fatalf("ingest: %v", err)
	}
	return resp
}

func (e *realEnv) waitDelivered(d *testkit.Deliverer, itemID int64) []string {
	e.t.Helper()
	var texts []string
	testkit.Eventually(e.t, 30*time.Second, func() bool {
		item, err := e.store.GetItem(context.Background(), itemID)
		if err != nil || item.State != state.StateTerminal {
			return false
		}
		texts = nil
		for i := 0; ; i++ {
			row, err := e.store.DeliveryForRun(context.Background(), item.RunID, i)
			if err != nil {
				break
			}
			if !row.Resolved() || row.DesiredRevision < 1 {
				return false
			}
			texts = append(texts, d.Text(row.RemoteID))
		}
		return len(texts) != 0
	}, fmt.Sprintf("item %d delivered", itemID))
	return texts
}

func discordDM(channel, actor, msgID, text string) state.Inbound {
	return state.Inbound{Route: state.Route{Platform: state.PlatformDiscord, Installation: "BOT", Channel: channel, DMActor: actor}, MessageID: msgID, Actor: actor, ActorLabel: actor, Content: text, ReceivedAt: time.Now()}
}

func discordThread(thread, guild, actor, msgID, text string) state.Inbound {
	return state.Inbound{Route: state.Route{Platform: state.PlatformDiscord, Installation: "BOT", Channel: thread, ThreadRoot: guild}, MessageID: msgID, Actor: actor, ActorLabel: actor, Content: text, ReceivedAt: time.Now()}
}

func messagesOf(req providerRequest) []map[string]any {
	var out []map[string]any
	if msgs, ok := req.Body["messages"].([]any); ok {
		for _, m := range msgs {
			mm, _ := m.(map[string]any)
			out = append(out, mm)
		}
	}
	return out
}

func roles(req providerRequest) string {
	var r []string
	for _, m := range messagesOf(req) {
		r = append(r, fmt.Sprint(m["role"]))
	}
	return strings.Join(r, ",")
}
