// Package testkit provides credential-free fixtures: a temporary state
// directory, a scripted provider streamer, a recording delivery adapter and
// an assembled conversation service.
package testkit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/model"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/render"
	"github.com/mattsp1290/eino-channels/internal/state"
)

// Behavior scripts one provider request.
type Behavior struct {
	// Reply is streamed as one delta per element.
	Reply []string
	// Block, when non-nil, pauses after the first delta until closed.
	Block chan struct{}
	// Started, when non-nil, is closed when the request begins streaming.
	Started chan struct{}
	// Err fails the request after any Reply deltas.
	Err error
	// ToolCall emits a tool call delta (unsupported output).
	ToolCall bool
}

// Script is a scripted model.Streamer that records requests.
type Script struct {
	mu        sync.Mutex
	behaviors []Behavior
	requests  []model.Request
	Default   Behavior
}

// NewScript builds a streamer whose default behavior echoes the last user message.
func NewScript() *Script {
	return &Script{Default: Behavior{}}
}

// Push queues a behavior for the next request.
func (s *Script) Push(b Behavior) {
	s.mu.Lock()
	s.behaviors = append(s.behaviors, b)
	s.mu.Unlock()
}

// Requests returns recorded provider requests.
func (s *Script) Requests() []model.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.Request(nil), s.requests...)
}

// StreamProvider implements model.Streamer.
func (s *Script) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	b := s.Default
	if len(s.behaviors) != 0 {
		b = s.behaviors[0]
		s.behaviors = s.behaviors[1:]
	}
	s.mu.Unlock()
	reply := b.Reply
	if reply == nil && b.Err == nil && !b.ToolCall {
		last := ""
		for i := len(request.Messages) - 1; i >= 0; i-- {
			if request.Messages[i].Role == einoschema.User {
				last = request.Messages[i].Content
				break
			}
		}
		reply = []string{"reply:" + last}
	}
	reader, writer := einoschema.Pipe[model.StreamDelta](4)
	go func() {
		defer writer.Close()
		if b.Started != nil {
			close(b.Started)
		}
		for i, part := range reply {
			if writer.Send(model.StreamDelta{Message: &einoschema.Message{Role: einoschema.Assistant, Content: part, Extra: map[string]any{"adapter": "x"}}}, nil) {
				return
			}
			if i == 0 && b.Block != nil {
				select {
				case <-b.Block:
				case <-ctx.Done():
					writer.Send(model.StreamDelta{}, ctx.Err())
					return
				}
			}
		}
		if b.ToolCall {
			writer.Send(model.StreamDelta{Message: einoschema.AssistantMessage("", []einoschema.ToolCall{{ID: "c1", Type: "function", Function: einoschema.FunctionCall{Name: "shell", Arguments: "{}"}}})}, nil)
			return
		}
		if b.Err != nil {
			writer.Send(model.StreamDelta{}, b.Err)
		}
	}()
	return reader, nil
}

// Call records one delivery call.
type Call struct {
	Op       string // create, edit, notify, reconcile
	Dest     state.Destination
	RemoteID string
	Text     string
	Nonce    string
}

// Deliverer records calls and injects failures.
type Deliverer struct {
	mu       sync.Mutex
	calls    []Call
	nextID   int
	messages map[string]string // remote id -> latest text
	// FailCreate, when non-nil, is consulted before each create.
	FailCreate func(dest state.Destination, text string) error
	// FailEdit, when non-nil, is consulted before each edit.
	FailEdit func(remoteID, text string) error
	// Reconciled, when set, answers reconcile lookups.
	Reconciled map[string]string // nonce -> remote id
	// Deny, when non-nil, rejects destinations.
	Deny func(state.Destination) bool
	// Budget is the chunk budget.
	Budget int
}

// NewDeliverer builds a recorder with a chunk budget.
func NewDeliverer(budget int) *Deliverer {
	return &Deliverer{messages: map[string]string{}, Budget: budget}
}

// Calls returns recorded calls.
func (d *Deliverer) Calls() []Call {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Call(nil), d.calls...)
}

// Text returns the latest text of a remote message.
func (d *Deliverer) Text(remoteID string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.messages[remoteID]
}

// Messages returns all remote messages in creation order.
func (d *Deliverer) Messages() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, d.nextID)
	for i := 1; i <= d.nextID; i++ {
		out = append(out, d.messages[fmt.Sprintf("m%d", i)])
	}
	return out
}

// Create implements conversation.Deliverer.
func (d *Deliverer) Create(_ context.Context, dest state.Destination, text, nonce string) (string, error) {
	d.mu.Lock()
	fail := d.FailCreate
	d.mu.Unlock()
	if fail != nil {
		if err := fail(dest, text); err != nil {
			d.mu.Lock()
			d.calls = append(d.calls, Call{Op: "create-failed", Dest: dest, Text: text, Nonce: nonce})
			d.mu.Unlock()
			return "", err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextID++
	id := fmt.Sprintf("m%d", d.nextID)
	d.messages[id] = text
	d.calls = append(d.calls, Call{Op: "create", Dest: dest, RemoteID: id, Text: text, Nonce: nonce})
	return id, nil
}

// Edit implements conversation.Deliverer.
func (d *Deliverer) Edit(_ context.Context, dest state.Destination, remoteID, text string) error {
	d.mu.Lock()
	fail := d.FailEdit
	d.mu.Unlock()
	if fail != nil {
		if err := fail(remoteID, text); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.messages[remoteID]; !ok {
		return &conversation.DeliveryError{Kind: conversation.KindPermanent, Err: errors.New("unknown message")}
	}
	d.messages[remoteID] = text
	d.calls = append(d.calls, Call{Op: "edit", Dest: dest, RemoteID: remoteID, Text: text})
	return nil
}

// Reconcile implements conversation.Deliverer.
func (d *Deliverer) Reconcile(_ context.Context, dest state.Destination, nonce string) (string, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, Call{Op: "reconcile", Dest: dest, Nonce: nonce})
	id, ok := d.Reconciled[nonce]
	return id, ok, nil
}

// Allowed implements conversation.Deliverer.
func (d *Deliverer) Allowed(_ context.Context, dest state.Destination) (bool, error) {
	d.mu.Lock()
	deny := d.Deny
	d.mu.Unlock()
	return deny == nil || !deny(dest), nil
}

// Notify implements conversation.Deliverer.
func (d *Deliverer) Notify(_ context.Context, dest state.Destination, text string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, Call{Op: "notify", Dest: dest, Text: text})
	return nil
}

// Chunks implements conversation.Deliverer.
func (d *Deliverer) Chunks(text string) []string {
	return render.Chunk(text, d.Budget, render.SlackMeasure)
}

// Preview implements conversation.Deliverer.
func (d *Deliverer) Preview(text string) string {
	return render.Preview(text, d.Budget, render.SlackMeasure)
}

// Notices returns notify texts.
func (d *Deliverer) Notices() []string {
	var out []string
	for _, c := range d.Calls() {
		if c.Op == "notify" {
			out = append(out, c.Text)
		}
	}
	return out
}

// Env is an assembled service over a state directory.
type Env struct {
	T         testing.TB
	Dir       string
	Store     *state.Store
	Bridge    *agentbridge.Bridge
	Service   *conversation.Service
	Deliverer *Deliverer
	Script    *Script
	Logs      *LogBuffer
	cancel    context.CancelFunc
}

// LogBuffer captures logs for sentinel assertions.
type LogBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *LogBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

// String returns the captured logs.
func (l *LogBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Options tune the environment.
type Options struct {
	Dir       string
	Limits    config.Limits
	Script    *Script
	Deliverer *Deliverer
	Platform  state.Platform
	Started   bool
}

// Open opens (or reopens) an environment. When Started is true the service
// is started immediately.
func Open(t testing.TB, opts Options) *Env {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	if opts.Script == nil {
		opts.Script = NewScript()
	}
	if opts.Deliverer == nil {
		opts.Deliverer = NewDeliverer(3500)
	}
	if opts.Platform == "" {
		opts.Platform = state.PlatformSlack
	}
	if opts.Limits.MaxRunningConversations == 0 {
		opts.Limits = config.DefaultLimits()
	}
	ctx := context.Background()
	st, err := state.Open(ctx, opts.Dir, state.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	bridge, err := agentbridge.New(ctx, agentbridge.Options{Store: st.Agent(), OwnerID: "test-" + fmt.Sprint(time.Now().UnixNano()), Resolver: agentbridge.FixedResolver{Streamer: agentbridge.NewStreamer(opts.Script)}})
	if err != nil {
		_ = st.Close()
		t.Fatalf("bridge: %v", err)
	}
	logs := &LogBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	svc, err := conversation.New(conversation.Options{Limits: opts.Limits, Store: st, Bridge: bridge, Deliverers: map[state.Platform]conversation.Deliverer{opts.Platform: opts.Deliverer, state.PlatformSlack: opts.Deliverer, state.PlatformDiscord: opts.Deliverer}, Logger: logger, DeliverySpacing: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("service: %v", err)
	}
	env := &Env{T: t, Dir: opts.Dir, Store: st, Bridge: bridge, Service: svc, Deliverer: opts.Deliverer, Script: opts.Script, Logs: logs}
	if tt, ok := t.(*testing.T); ok {
		tt.Cleanup(func() {
			if tt.Failed() {
				tt.Logf("service logs:\n%s", logs.String())
			}
		})
	}
	if opts.Started {
		env.Start()
	}
	return env
}

// Start starts the service.
func (e *Env) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.Service.Start(ctx)
}

// Close shuts down the service and closes the state. It is safe to call twice.
func (e *Env) Close() {
	if e.Store == nil {
		return
	}
	if e.cancel != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := e.Service.Shutdown(ctx); err != nil {
			e.T.Logf("shutdown: %v", err)
		}
		cancel()
		e.cancel()
	}
	_ = e.Bridge.Close(context.Background())
	_ = e.Store.Close()
	e.Store = nil
}

// Inbound builds a Slack shared-thread prompt.
func Inbound(channel, thread, actor, msgID, text string) state.Inbound {
	return state.Inbound{Route: state.Route{Platform: state.PlatformSlack, Installation: "T1", Channel: channel, ThreadRoot: thread}, MessageID: msgID, Actor: actor, ActorLabel: actor, Content: text, ReceivedAt: time.Now()}
}

// DM builds a Slack DM prompt.
func DM(channel, actor, msgID, text string) state.Inbound {
	return state.Inbound{Route: state.Route{Platform: state.PlatformSlack, Installation: "T1", Channel: channel, DMActor: actor}, MessageID: msgID, Actor: actor, ActorLabel: actor, Content: text, ReceivedAt: time.Now()}
}

// Eventually polls until fn returns true or the deadline passes.
func Eventually(t testing.TB, timeout time.Duration, fn func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", msg)
}

// WaitDelivered waits until the delivery for the inbox item is acknowledged
// at its final revision and returns the delivered chunk texts.
func (e *Env) WaitDelivered(itemID int64, timeout time.Duration) []string {
	e.T.Helper()
	var texts []string
	Eventually(e.T, timeout, func() bool {
		item, err := e.Store.GetItem(context.Background(), itemID)
		if err != nil || item.State != state.StateTerminal {
			return false
		}
		texts = nil
		for i := 0; ; i++ {
			d, err := e.Store.DeliveryForRun(context.Background(), item.RunID, i)
			if err != nil {
				break
			}
			if !d.Resolved() || d.DesiredRevision < 1 {
				return false
			}
			texts = append(texts, e.Deliverer.Text(d.RemoteID))
		}
		return len(texts) != 0
	}, fmt.Sprintf("item %d delivered", itemID))
	return texts
}

// Ingest submits an inbound event and fails the test on error.
func (e *Env) Ingest(in state.Inbound) conversation.Response {
	e.T.Helper()
	resp, err := e.Service.Ingest(context.Background(), in)
	if err != nil {
		e.T.Fatalf("ingest: %v", err)
	}
	return resp
}
