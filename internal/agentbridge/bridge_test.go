package agentbridge_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/state"
)

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

func newTestState(t *testing.T) *state.Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	st, err := state.Open(context.Background(), dir, state.Options{})
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func newRealBridge(t *testing.T, st *state.Store, baseURL string) *agentbridge.Bridge {
	t.Helper()
	b, err := agentbridge.New(context.Background(), agentbridge.Options{
		Store:     st.Agent(),
		OwnerID:   "t",
		APIKey:    "SENTINEL_KEY_abc",
		UserAgent: "eino-channels/test",
		BaseURL:   baseURL,
	})
	if err != nil {
		t.Fatalf("agentbridge.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	return b
}

func waitDone(t *testing.T, h runtime.Handle) runtime.Result {
	t.Helper()
	if h == nil {
		t.Fatal("waitDone: nil handle")
	}
	select {
	case r := <-h.Done():
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for run to finish")
	}
	return runtime.Result{}
}

// recordedRequest captures what the fake OpenCode Go server observed.
type recordedRequest struct {
	path   string
	header http.Header
	body   map[string]any
}

// scriptedServer records every request it receives and delegates response
// writing to a per-test handler.
type scriptedServer struct {
	mu     sync.Mutex
	reqs   []recordedRequest
	handle func(w http.ResponseWriter, r *http.Request, index int)
}

func newScriptedServer(t *testing.T, handle func(w http.ResponseWriter, r *http.Request, index int)) (*scriptedServer, *httptest.Server) {
	t.Helper()
	s := &scriptedServer{handle: handle}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		s.mu.Lock()
		index := len(s.reqs)
		s.reqs = append(s.reqs, recordedRequest{path: r.URL.Path, header: r.Header.Clone(), body: body})
		s.mu.Unlock()
		s.handle(w, r, index)
	}))
	t.Cleanup(httpSrv.Close)
	return s, httpSrv
}

func (s *scriptedServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.reqs)
}

func (s *scriptedServer) request(i int) recordedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reqs[i]
}

func baseURLFor(httpSrv *httptest.Server) string {
	return httpSrv.URL + "/zen/go/v1"
}

// ---------------------------------------------------------------------------
// SSE / HTTP scenario handlers
// ---------------------------------------------------------------------------

func writeSSELine(w http.ResponseWriter, payload string) {
	fmt.Fprintf(w, "data: %s\n\n", payload)
}

func normalSSEHandler(w http.ResponseWriter, _ *http.Request, _ int) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl := w.(http.Flusher)
	writeSSELine(w, `{"id":"id","object":"chat.completion.chunk","created":0,"model":"deepseek-v4-flash","choices":[{"index":0,"delta":{"role":"assistant","content":"hello "}}]}`)
	fl.Flush()
	writeSSELine(w, `{"id":"id","object":"chat.completion.chunk","created":0,"model":"deepseek-v4-flash","choices":[{"index":0,"delta":{"content":"world"},"finish_reason":"stop"}]}`)
	fl.Flush()
	writeSSELine(w, "[DONE]")
	fl.Flush()
}

func toolCallSSEHandler(w http.ResponseWriter, _ *http.Request, _ int) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl := w.(http.Flusher)
	writeSSELine(w, `{"id":"id","object":"chat.completion.chunk","created":0,"model":"deepseek-v4-flash","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","type":"function","function":{"name":"shell","arguments":"{}"}}]}}]}`)
	fl.Flush()
	writeSSELine(w, "[DONE]")
	fl.Flush()
}

func truncatedSSEHandler(w http.ResponseWriter, _ *http.Request, _ int) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl := w.(http.Flusher)
	writeSSELine(w, `{"id":"id","object":"chat.completion.chunk","created":0,"model":"deepseek-v4-flash","choices":[{"index":0,"delta":{"role":"assistant","content":"partial"}}]}`)
	fl.Flush()
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	// Abruptly close the raw connection mid-stream (no closing chunk, no
	// [DONE] marker) so the client observes a genuine truncation error
	// rather than a clean end-of-stream.
	_ = conn.Close()
}

func slowHandler(w http.ResponseWriter, r *http.Request, _ int) {
	select {
	case <-time.After(5 * time.Second):
	case <-r.Context().Done():
	}
}

func jsonErrorHandler(status int, body string, headers map[string]string) func(w http.ResponseWriter, r *http.Request, index int) {
	return func(w http.ResponseWriter, _ *http.Request, _ int) {
		for k, v := range headers {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// ---------------------------------------------------------------------------
// 1 + 2: wire correctness (headers, body shape, committed text, history)
// ---------------------------------------------------------------------------

func TestBridgeWireAndHistory(t *testing.T) {
	st := newTestState(t)
	ss, httpSrv := newScriptedServer(t, normalSSEHandler)
	b := newRealBridge(t, st, baseURLFor(httpSrv))
	ctx := context.Background()

	res, err := b.Submit(ctx, agentbridge.Turn{SessionID: "sess-1", AdmissionKey: "evt-1", Content: "hi", WorkspaceID: "w"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if res.Disposition != runtime.AdmissionNew || res.Handle == nil {
		t.Fatalf("unexpected admission result: %+v", res)
	}
	r := waitDone(t, res.Handle)
	if r.Status != session.RunCompleted {
		t.Fatalf("run status = %v, error = %v", r.Status, r.Error)
	}

	if got := ss.count(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
	req := ss.request(0)
	if req.path != "/zen/go/v1/chat/completions" {
		t.Fatalf("path = %q", req.path)
	}
	if got := req.header.Get("User-Agent"); got != "eino-channels/test" {
		t.Fatalf("User-Agent = %q", got)
	}
	if got := req.header.Get("Authorization"); got != "Bearer SENTINEL_KEY_abc" {
		t.Fatalf("Authorization = %q", got)
	}
	wantSession := agentbridge.ProviderSessionID("sess-1")
	if !strings.HasPrefix(wantSession, "ecs-") || len(wantSession) != 36 {
		t.Fatalf("ProviderSessionID shape = %q", wantSession)
	}
	for _, c := range wantSession[len("ecs-"):] {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("ProviderSessionID suffix not hex: %q", wantSession)
		}
	}
	if got := req.header.Get("x-opencode-session"); got != wantSession {
		t.Fatalf("x-opencode-session = %q, want %q", got, wantSession)
	}
	if other := agentbridge.ProviderSessionID("sess-2"); other == wantSession {
		t.Fatalf("ProviderSessionID did not differ across sessions")
	}

	if got, _ := req.body["model"].(string); got != config.ModelID {
		t.Fatalf("model = %v", req.body["model"])
	}
	if got, _ := req.body["stream"].(bool); !got {
		t.Fatalf("stream = %v", req.body["stream"])
	}
	if _, ok := req.body["tools"]; ok {
		t.Fatalf("unexpected tools key in body: %v", req.body["tools"])
	}
	if _, ok := req.body["tool_choice"]; ok {
		t.Fatalf("unexpected tool_choice key in body: %v", req.body["tool_choice"])
	}
	maxTokens, ok := req.body["max_tokens"]
	if !ok {
		t.Fatalf("missing max_tokens key")
	}
	if mt, _ := maxTokens.(float64); mt != float64(config.MaxOutputTokens) {
		t.Fatalf("max_tokens = %v, want %d", maxTokens, config.MaxOutputTokens)
	}
	msgs, ok := req.body["messages"].([]any)
	if !ok || len(msgs) != 2 {
		t.Fatalf("messages = %#v", req.body["messages"])
	}
	msg0, _ := msgs[0].(map[string]any)
	if msg0["role"] != "system" || msg0["content"] != config.SystemPrompt {
		t.Fatalf("messages[0] = %#v", msg0)
	}
	msg1, _ := msgs[1].(map[string]any)
	if msg1["role"] != "user" || msg1["content"] != "hi" {
		t.Fatalf("messages[1] = %#v", msg1)
	}

	snap, err := b.Committed(ctx, "sess-1")
	if err != nil {
		t.Fatalf("Committed: %v", err)
	}
	text, ok := agentbridge.AssistantText(snap, res.Receipt.RunID)
	if !ok || text != "hello world" {
		t.Fatalf("AssistantText = %q, ok=%v", text, ok)
	}

	// --- Second turn: history must replay exactly once each. ---
	res2, err := b.Submit(ctx, agentbridge.Turn{SessionID: "sess-1", AdmissionKey: "evt-2", Content: "again", WorkspaceID: "w"})
	if err != nil {
		t.Fatalf("Submit (2): %v", err)
	}
	r2 := waitDone(t, res2.Handle)
	if r2.Status != session.RunCompleted {
		t.Fatalf("run 2 status = %v, error = %v", r2.Status, r2.Error)
	}
	if got := ss.count(); got != 2 {
		t.Fatalf("request count after turn 2 = %d, want 2", got)
	}
	req2 := ss.request(1)
	msgs2, ok := req2.body["messages"].([]any)
	if !ok || len(msgs2) != 4 {
		t.Fatalf("turn 2 messages = %#v", req2.body["messages"])
	}
	wantRoles := []string{"system", "user", "assistant", "user"}
	wantContents := []string{config.SystemPrompt, "hi", "hello world", "again"}
	for i, m := range msgs2 {
		entry, _ := m.(map[string]any)
		if entry["role"] != wantRoles[i] || entry["content"] != wantContents[i] {
			t.Fatalf("turn 2 messages[%d] = %#v, want role=%q content=%q", i, entry, wantRoles[i], wantContents[i])
		}
	}
}

// ---------------------------------------------------------------------------
// 3: duplicate admission, lookup, and conflict
// ---------------------------------------------------------------------------

func TestBridgeDuplicateAdmissionAndLookup(t *testing.T) {
	st := newTestState(t)
	ss, httpSrv := newScriptedServer(t, normalSSEHandler)
	b := newRealBridge(t, st, baseURLFor(httpSrv))
	ctx := context.Background()

	turn := agentbridge.Turn{SessionID: "sess-1", AdmissionKey: "evt-1", Content: "hi", WorkspaceID: "w"}
	res, err := b.Submit(ctx, turn)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	r := waitDone(t, res.Handle)
	if r.Status != session.RunCompleted {
		t.Fatalf("run status = %v, error = %v", r.Status, r.Error)
	}

	dup, err := b.Submit(ctx, turn)
	if err != nil {
		t.Fatalf("duplicate Submit: %v", err)
	}
	if dup.Disposition != runtime.AdmissionExisting || dup.Handle != nil {
		t.Fatalf("duplicate admission result = %+v", dup)
	}
	if dup.Receipt != res.Receipt {
		t.Fatalf("duplicate receipt = %+v, want %+v", dup.Receipt, res.Receipt)
	}
	if got := ss.count(); got != 1 {
		t.Fatalf("request count after duplicate submit = %d, want 1", got)
	}

	if _, err := b.Lookup(ctx, "sess-1", "nope"); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("Lookup missing key error = %v, want ErrNotFound", err)
	}

	_, err = b.Submit(ctx, agentbridge.Turn{SessionID: "sess-1", AdmissionKey: "evt-1", Content: "different content", WorkspaceID: "w"})
	if !errors.Is(err, session.ErrAdmissionConflict) {
		t.Fatalf("changed-content Submit error = %v, want ErrAdmissionConflict", err)
	}

	rec, err := b.Lookup(ctx, "sess-1", "evt-1")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if rec.Receipt != res.Receipt {
		t.Fatalf("looked up receipt = %+v, want %+v", rec.Receipt, res.Receipt)
	}
	if rec.RunStatus != session.RunCompleted {
		t.Fatalf("looked up run status = %v", rec.RunStatus)
	}
}

// ---------------------------------------------------------------------------
// 4: failures settle failed without retry, and never leak secrets
// ---------------------------------------------------------------------------

func TestBridgeFailuresNoRetryNoLeak(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		headers map[string]string
	}{
		{name: "401", status: http.StatusUnauthorized, body: `{"error":{"message":"SENTINEL_BODY bad key","type":"authentication_error"}}`},
		{name: "429", status: http.StatusTooManyRequests, body: `{"error":{"message":"SENTINEL_BODY rate limited","type":"rate_limit_error"}}`, headers: map[string]string{"Retry-After": "1"}},
		{name: "402", status: http.StatusPaymentRequired, body: `{"error":{"message":"SENTINEL_BODY quota exceeded","type":"insufficient_quota"}}`},
		{name: "500", status: http.StatusInternalServerError, body: `{"error":{"message":"SENTINEL_BODY server error","type":"server_error"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestState(t)
			ss, httpSrv := newScriptedServer(t, jsonErrorHandler(tc.status, tc.body, tc.headers))
			b := newRealBridge(t, st, baseURLFor(httpSrv))
			ctx := context.Background()

			res, err := b.Submit(ctx, agentbridge.Turn{SessionID: "sess-1", AdmissionKey: "evt-1", Content: "hi", WorkspaceID: "w"})
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}
			r := waitDone(t, res.Handle)
			if r.Status != session.RunFailed {
				t.Fatalf("run status = %v, error = %v", r.Status, r.Error)
			}
			if got := ss.count(); got != 1 {
				t.Fatalf("request count = %d, want 1", got)
			}
			if r.Error == nil {
				t.Fatalf("expected a non-nil run error")
			}
			errText := r.Error.Error()
			plusV := fmt.Sprintf("%+v", r.Error)
			for _, sentinel := range []string{"SENTINEL_BODY", "SENTINEL_KEY"} {
				if strings.Contains(errText, sentinel) {
					t.Fatalf("Error() leaked sentinel %q: %q", sentinel, errText)
				}
				if strings.Contains(plusV, sentinel) {
					t.Fatalf("%%+v leaked sentinel %q: %q", sentinel, plusV)
				}
			}
		})
	}
}

func TestBridgeDeadlineExceeded(t *testing.T) {
	st := newTestState(t)
	ss, httpSrv := newScriptedServer(t, slowHandler)
	b := newRealBridge(t, st, baseURLFor(httpSrv))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	res, err := b.Submit(ctx, agentbridge.Turn{SessionID: "sess-1", AdmissionKey: "evt-1", Content: "hi", WorkspaceID: "w"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	r := waitDone(t, res.Handle)
	if r.Status != session.RunFailed && r.Status != session.RunInterrupted {
		t.Fatalf("run status = %v, error = %v", r.Status, r.Error)
	}
	if got := ss.count(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
}

func TestBridgeTruncatedSSE(t *testing.T) {
	st := newTestState(t)
	ss, httpSrv := newScriptedServer(t, truncatedSSEHandler)
	b := newRealBridge(t, st, baseURLFor(httpSrv))
	ctx := context.Background()

	res, err := b.Submit(ctx, agentbridge.Turn{SessionID: "sess-1", AdmissionKey: "evt-1", Content: "hi", WorkspaceID: "w"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	r := waitDone(t, res.Handle)
	if r.Status == session.RunCompleted {
		t.Fatalf("truncated stream must not complete, got status = %v", r.Status)
	}
	if r.Status != session.RunFailed && r.Status != session.RunInterrupted {
		t.Fatalf("run status = %v, error = %v", r.Status, r.Error)
	}
	if got := ss.count(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
}

func TestBridgeToolCallOutputUnsupported(t *testing.T) {
	st := newTestState(t)
	ss, httpSrv := newScriptedServer(t, toolCallSSEHandler)
	b := newRealBridge(t, st, baseURLFor(httpSrv))
	ctx := context.Background()

	res, err := b.Submit(ctx, agentbridge.Turn{SessionID: "sess-1", AdmissionKey: "evt-1", Content: "hi", WorkspaceID: "w"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	r := waitDone(t, res.Handle)
	if r.Status != session.RunFailed {
		t.Fatalf("run status = %v, error = %v", r.Status, r.Error)
	}
	if got := ss.count(); got != 1 {
		t.Fatalf("request count = %d, want 1", got)
	}
	if r.Error == nil {
		t.Fatalf("expected a non-nil run error")
	}
	if !errors.Is(r.Error, agentbridge.ErrUnsupportedOutput) && !strings.Contains(strings.ToLower(r.Error.Error()), "unsupported output") {
		t.Fatalf("run error = %v, want ErrUnsupportedOutput or a message containing \"unsupported output\"", r.Error)
	}
}

// ---------------------------------------------------------------------------
// 5: Streamer wrapper unit tests (no HTTP)
// ---------------------------------------------------------------------------

// fakeStreamer is a minimal model.Streamer used to unit test agentbridge.Streamer
// without any network transport.
type fakeStreamer struct {
	deltas []model.StreamDelta
	err    error
}

func (f fakeStreamer) StreamProvider(context.Context, model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	if f.err != nil {
		return nil, f.err
	}
	return einoschema.StreamReaderFromArray(append([]model.StreamDelta(nil), f.deltas...)), nil
}

func TestStreamerToolsNotAllowed(t *testing.T) {
	s := agentbridge.NewStreamer(fakeStreamer{})
	_, err := s.StreamProvider(context.Background(), model.Request{
		Messages: []*einoschema.Message{einoschema.UserMessage("hi")},
		Tools:    []*einoschema.ToolInfo{{Name: "x"}},
		Identity: model.Identity{SessionID: "s"},
	})
	if !errors.Is(err, agentbridge.ErrToolsNotAllowed) {
		t.Fatalf("err = %v, want ErrToolsNotAllowed", err)
	}
}

func TestStreamerRequestTooLarge(t *testing.T) {
	s := agentbridge.NewStreamer(fakeStreamer{})
	big := strings.Repeat("a", (1<<20)+1)
	_, err := s.StreamProvider(context.Background(), model.Request{
		Messages: []*einoschema.Message{einoschema.UserMessage(big)},
		Identity: model.Identity{SessionID: "s"},
	})
	if !errors.Is(err, agentbridge.ErrRequestTooLarge) {
		t.Fatalf("err = %v, want ErrRequestTooLarge", err)
	}
}

func TestStreamerEmptySessionID(t *testing.T) {
	s := agentbridge.NewStreamer(fakeStreamer{})
	_, err := s.StreamProvider(context.Background(), model.Request{
		Messages: []*einoschema.Message{einoschema.UserMessage("hi")},
	})
	if err == nil {
		t.Fatal("expected an error for empty Identity.SessionID")
	}
}

func TestStreamerExtraStripped(t *testing.T) {
	inner := fakeStreamer{deltas: []model.StreamDelta{{Message: &einoschema.Message{
		Role: einoschema.Assistant, Content: "hi", Extra: map[string]any{"x": 1},
	}}}}
	s := agentbridge.NewStreamer(inner)
	reader, err := s.StreamProvider(context.Background(), model.Request{
		Messages: []*einoschema.Message{einoschema.UserMessage("hi")},
		Identity: model.Identity{SessionID: "s"},
	})
	if err != nil {
		t.Fatalf("StreamProvider: %v", err)
	}
	defer reader.Close()
	delta, err := reader.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if delta.Message == nil || delta.Message.Extra != nil {
		t.Fatalf("Extra not stripped: %#v", delta.Message)
	}
	if delta.Message.Content != "hi" {
		t.Fatalf("content = %q", delta.Message.Content)
	}
}

func TestStreamerToolCallDeltaUnsupported(t *testing.T) {
	inner := fakeStreamer{deltas: []model.StreamDelta{{Message: &einoschema.Message{
		Role: einoschema.Assistant,
		ToolCalls: []einoschema.ToolCall{{
			ID: "c1", Type: "function", Function: einoschema.FunctionCall{Name: "shell", Arguments: "{}"},
		}},
	}}}}
	s := agentbridge.NewStreamer(inner)
	reader, err := s.StreamProvider(context.Background(), model.Request{
		Messages: []*einoschema.Message{einoschema.UserMessage("hi")},
		Identity: model.Identity{SessionID: "s"},
	})
	if err != nil {
		t.Fatalf("StreamProvider: %v", err)
	}
	defer reader.Close()
	_, err = reader.Recv()
	if !errors.Is(err, agentbridge.ErrUnsupportedOutput) {
		t.Fatalf("Recv err = %v, want ErrUnsupportedOutput", err)
	}
}

// ---------------------------------------------------------------------------
// 6: FixedResolver
// ---------------------------------------------------------------------------

func TestFixedResolver(t *testing.T) {
	r := agentbridge.FixedResolver{Streamer: agentbridge.NewStreamer(fakeStreamer{})}

	resolved, err := r.Resolve(context.Background(), model.Selection{
		ProviderID: model.ProviderID(config.ProviderID),
		ModelID:    model.ID(config.ModelID),
	}, model.Runtime{})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if string(resolved.Provider.ID) != config.ProviderID {
		t.Fatalf("Provider.ID = %q, want %q", resolved.Provider.ID, config.ProviderID)
	}
	if string(resolved.Model.ID) != config.ModelID {
		t.Fatalf("Model.ID = %q, want %q", resolved.Model.ID, config.ModelID)
	}

	if _, err := r.Resolve(context.Background(), model.Selection{ProviderID: "wrong", ModelID: "wrong"}, model.Runtime{}); err == nil {
		t.Fatal("expected an error for an unsupported selection")
	}
}

// ---------------------------------------------------------------------------
// 7: HistoryWithinLimits
// ---------------------------------------------------------------------------

// echo is a fake model.Streamer that always replies with a fixed assistant
// message, bypassing HTTP entirely (used with agentbridge.FixedResolver).
type echo struct{ text string }

func (e *echo) StreamProvider(context.Context, model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	text := e.text
	if text == "" {
		text = "ok"
	}
	msg := &einoschema.Message{Role: einoschema.Assistant, Content: text}
	return einoschema.StreamReaderFromArray([]model.StreamDelta{{Message: msg}}), nil
}

func newFastBridge(t *testing.T, st *state.Store, streamer model.Streamer) *agentbridge.Bridge {
	t.Helper()
	b, err := agentbridge.New(context.Background(), agentbridge.Options{
		Store:    st.Agent(),
		OwnerID:  "t",
		Resolver: agentbridge.FixedResolver{Streamer: agentbridge.NewStreamer(streamer)},
	})
	if err != nil {
		t.Fatalf("agentbridge.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	return b
}

func TestHistoryWithinLimitsFreshSession(t *testing.T) {
	st := newTestState(t)
	b := newFastBridge(t, st, &echo{})
	ok, err := b.HistoryWithinLimits(context.Background(), "brand-new-session")
	if err != nil {
		t.Fatalf("HistoryWithinLimits: %v", err)
	}
	if !ok {
		t.Fatal("expected true for a fresh session")
	}
}

func TestHistoryWithinLimitsHundredTurns(t *testing.T) {
	if raceEnabled {
		t.Skip("long: runs in the non-race pass")
	}
	st := newTestState(t)
	b := newFastBridge(t, st, &echo{})
	ctx := context.Background()

	start := time.Now()
	for i := 1; i <= 100; i++ {
		key := fmt.Sprintf("evt-%d", i)
		res, err := b.Submit(ctx, agentbridge.Turn{SessionID: "hist-sess", AdmissionKey: key, Content: "hi", WorkspaceID: "w"})
		if err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
		r := waitDone(t, res.Handle)
		if r.Status != session.RunCompleted {
			t.Fatalf("turn %d status = %v, error = %v", i, r.Status, r.Error)
		}
		if i == 99 {
			ok, err := b.HistoryWithinLimits(ctx, "hist-sess")
			if err != nil {
				t.Fatalf("HistoryWithinLimits after 99: %v", err)
			}
			if !ok {
				t.Fatal("expected true after 99 turns")
			}
		}
	}
	elapsed := time.Since(start)
	t.Logf("100 turns took %v", elapsed)

	ok, err := b.HistoryWithinLimits(ctx, "hist-sess")
	if err != nil {
		t.Fatalf("HistoryWithinLimits after 100: %v", err)
	}
	if ok {
		t.Fatal("expected false after 100 turns")
	}
}

func TestHistoryWithinLimitsOversizedReply(t *testing.T) {
	st := newTestState(t)
	big := strings.Repeat("y", 300<<10)
	b := newFastBridge(t, st, &echo{text: big})
	ctx := context.Background()

	res, err := b.Submit(ctx, agentbridge.Turn{SessionID: "big-sess", AdmissionKey: "evt-1", Content: "hi", WorkspaceID: "w"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	r := waitDone(t, res.Handle)
	if r.Status != session.RunCompleted {
		t.Fatalf("run status = %v, error = %v", r.Status, r.Error)
	}

	ok, err := b.HistoryWithinLimits(ctx, "big-sess")
	if err != nil {
		t.Fatalf("HistoryWithinLimits: %v", err)
	}
	if ok {
		t.Fatal("expected false for an oversized assistant reply")
	}
}

// ---------------------------------------------------------------------------
// 8: IDs uniqueness
// ---------------------------------------------------------------------------

func TestIDsUnique(t *testing.T) {
	ids := agentbridge.IDs{}
	runSeen := make(map[session.RunID]bool, 1000)
	msgSeen := make(map[session.MessageID]bool, 1000)
	partSeen := make(map[session.PartID]bool, 1000)
	evtSeen := make(map[session.EventID]bool, 1000)
	epochSeen := make(map[session.EpochID]bool, 1000)
	callSeen := make(map[session.ToolCallID]bool, 1000)

	for i := 0; i < 1000; i++ {
		if r := ids.NewRunID(); runSeen[r] {
			t.Fatalf("duplicate run id %q at iteration %d", r, i)
		} else {
			runSeen[r] = true
			if !strings.HasPrefix(string(r), "run-") {
				t.Fatalf("run id %q missing prefix", r)
			}
		}
		if m := ids.NewMessageID(); msgSeen[m] {
			t.Fatalf("duplicate message id %q at iteration %d", m, i)
		} else {
			msgSeen[m] = true
			if !strings.HasPrefix(string(m), "msg-") {
				t.Fatalf("message id %q missing prefix", m)
			}
		}
		if p := ids.NewPartID(); partSeen[p] {
			t.Fatalf("duplicate part id %q at iteration %d", p, i)
		} else {
			partSeen[p] = true
			if !strings.HasPrefix(string(p), "part-") {
				t.Fatalf("part id %q missing prefix", p)
			}
		}
		if e := ids.NewEventID(); evtSeen[e] {
			t.Fatalf("duplicate event id %q at iteration %d", e, i)
		} else {
			evtSeen[e] = true
			if !strings.HasPrefix(string(e), "evt-") {
				t.Fatalf("event id %q missing prefix", e)
			}
		}
		if ep := ids.NewEpochID(); epochSeen[ep] {
			t.Fatalf("duplicate epoch id %q at iteration %d", ep, i)
		} else {
			epochSeen[ep] = true
			if !strings.HasPrefix(string(ep), "epoch-") {
				t.Fatalf("epoch id %q missing prefix", ep)
			}
		}
		if c := ids.NewToolCallID(); callSeen[c] {
			t.Fatalf("duplicate tool call id %q at iteration %d", c, i)
		} else {
			callSeen[c] = true
			if !strings.HasPrefix(string(c), "call-") {
				t.Fatalf("tool call id %q missing prefix", c)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 9: WatchOptions / ObservationLimits validity
// ---------------------------------------------------------------------------

func TestWatchOptionsAndObservationLimitsValid(t *testing.T) {
	if err := agentbridge.WatchOptions().Snapshot.Validate(); err != nil {
		t.Fatalf("WatchOptions().Snapshot.Validate() = %v", err)
	}
	st := newTestState(t)
	svc, err := watch.NewService(st.Agent(), agentbridge.WatchOptions())
	if err != nil {
		t.Fatalf("watch.NewService: %v", err)
	}
	if err := svc.Close(context.Background()); err != nil {
		t.Fatalf("Service.Close: %v", err)
	}
}
