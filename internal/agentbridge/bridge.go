// Package agentbridge embeds eino-agent for the channel host: a host-owned
// SQLite store, a bounded watch service, an empty tool registry, the fixed
// OpenCode Go / DeepSeek V4 Flash resolver and a tool-free stream wrapper.
package agentbridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/composition"
	agentconfig "github.com/mattsp1290/eino-agent/config"
	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"
	"github.com/mattsp1290/eino-providers/opencodego"
	opencodeauth "github.com/mattsp1290/opencode-auth-go"

	"github.com/mattsp1290/eino-channels/internal/config"
)

// Errors surfaced with fixed safe text.
var (
	ErrUnsupportedOutput = errors.New("model produced unsupported output")
	ErrRequestTooLarge   = errors.New("provider request exceeds the size limit")
	ErrToolsNotAllowed   = errors.New("tools are not supported")
	ErrHistoryLimit      = errors.New("conversation history limit reached")
)

// Store is the durable store contract the host requires: the transactional
// session store plus committed observation reads.
type Store interface {
	session.Store
	session.ObservationReader
}

// Options configure the bridge.
type Options struct {
	Store   Store
	OwnerID string
	Lease   time.Duration
	// Provider credentials and identity.
	APIKey     string
	UserAgent  string
	BaseURL    string // empty selects the OpenCode Go default
	HTTPClient *http.Client
	// Resolver overrides the fixed provider resolver (tests only).
	Resolver model.Resolver
	// IDs overrides the ID generator (tests only).
	IDs runtime.IDGenerator
}

// Bridge owns the runtime pieces.
type Bridge struct {
	store    Store
	registry *composition.Registry
	watch    *watch.Service
	orch     *runtime.StreamingOrchestrator
}

// WatchOptions are the bounded observation limits used by the host. They
// are sized to contain the maximum supported generation (100 turns).
func WatchOptions() watch.Options {
	return watch.Options{
		Snapshot: ObservationLimits(),
		// Both databases run on single-connection pools, so polls compete
		// with orchestrator writes; a read timeout kills the subscription.
		PollInterval:       100 * time.Millisecond,
		ReadTimeout:        5 * time.Second,
		MaxSubscriptions:   64,
		MaxWatchedSessions: 32,
		MaxLiveRuns:        32,
		MaxLiveTextBytes:   1 << 20,
		PendingUpdates:     64,
	}
}

// ObservationLimits bound snapshot reads.
func ObservationLimits() session.ObservationLimits {
	return session.ObservationLimits{MaxMessages: 200, MaxTools: 1, MaxParts: 400, MaxSnapshotBytes: 4 << 20, MaxTextBytes: 320 << 10}
}

// New builds the bridge.
func New(ctx context.Context, opts Options) (*Bridge, error) {
	if opts.Store == nil {
		return nil, errors.New("agentbridge: store required")
	}
	if opts.OwnerID == "" {
		return nil, errors.New("agentbridge: owner ID required")
	}
	if opts.Lease <= 0 {
		opts.Lease = config.AgentLeaseSeconds * time.Second
	}
	resolver := opts.Resolver
	if resolver == nil {
		if opts.APIKey == "" || opts.UserAgent == "" {
			return nil, errors.New("agentbridge: provider credentials required")
		}
		maxTokens := config.MaxOutputTokens
		client, err := opencodego.NewChatModel(ctx, opencodego.ChatModelConfig{
			Model: config.ModelID, Protocol: opencodego.ProtocolChatCompletions, APIKey: opts.APIKey,
			UserAgent: opts.UserAgent, BaseURL: opts.BaseURL, HTTPClient: opts.HTTPClient, MaxTokens: &maxTokens,
		})
		if err != nil {
			return nil, fmt.Errorf("agentbridge: build provider: %w", err)
		}
		resolver = FixedResolver{Streamer: NewStreamer(model.NewEinoStreamer(client))}
	}
	ids := opts.IDs
	if ids == nil {
		ids = IDs{}
	}
	registry, err := composition.NewRegistry(nil)
	if err != nil {
		return nil, fmt.Errorf("agentbridge: registry: %w", err)
	}
	observer, err := watch.NewService(opts.Store, WatchOptions())
	if err != nil {
		return nil, fmt.Errorf("agentbridge: watch: %w", err)
	}
	orch, err := runtime.NewStreamingOrchestrator(
		runtime.WithStore(opts.Store), runtime.WithModelResolver(resolver), runtime.WithIDGenerator(ids),
		runtime.WithRunPlanProvider(registry), runtime.WithSessionObserver(observer), runtime.WithAttempts(1),
		runtime.WithOwnerID(opts.OwnerID), runtime.WithLease(opts.Lease),
	)
	if err != nil {
		_ = observer.Close(context.Background())
		return nil, fmt.Errorf("agentbridge: orchestrator: %w", err)
	}
	return &Bridge{store: opts.Store, registry: registry, watch: observer, orch: orch}, nil
}

// Close stops observation. The caller closes the store pool afterwards.
func (b *Bridge) Close(ctx context.Context) error { return b.watch.Close(ctx) }

// Selection is the sole model selection.
func Selection() model.Selection {
	return model.Selection{ProviderID: config.ProviderID, ModelID: config.ModelID}
}

// Snapshot builds the frozen configuration for a route. workspaceID is the
// stable route identity carried in Metadata; it never changes per message.
func Snapshot(workspaceID string) agentconfig.Snapshot {
	sel := Selection()
	return agentconfig.Snapshot{
		Agent:    agentconfig.Agent{Name: config.AgentName, SystemPrompt: config.SystemPrompt, Model: sel},
		Model:    sel,
		Metadata: map[string]string{"workspace_id": workspaceID},
	}
}

// Turn is one frozen keyed submission.
type Turn struct {
	SessionID    session.ID
	AdmissionKey string
	Content      string
	WorkspaceID  string
}

// Request builds the runtime request for a turn.
func (t Turn) Request() runtime.Request {
	return runtime.Request{SessionID: t.SessionID, AdmissionKey: t.AdmissionKey, Message: runtime.UserMessage{Content: t.Content}, Config: Snapshot(t.WorkspaceID)}
}

// Submit starts (or resolves) a keyed turn.
func (b *Bridge) Submit(ctx context.Context, turn Turn) (runtime.AdmissionResult, error) {
	return b.orch.Start(ctx, turn.Request())
}

// Lookup resolves a committed receipt through the root store, outside any
// transaction and without constructing execution. Unlike runtime.Start it
// does not compare the fingerprint version: a receipt from an older
// fingerprint version is still adopted because that run really happened.
func (b *Bridge) Lookup(ctx context.Context, sessionID session.ID, key string) (session.AdmissionRecord, error) {
	return b.store.LookupAdmission(ctx, sessionID, key)
}

// Watch subscribes to a session.
func (b *Bridge) Watch(ctx context.Context, sessionID session.ID) (*watch.Subscription, error) {
	return b.watch.Watch(ctx, sessionID)
}

// Resume reclaims an abandoned run. With no tools it settles as interrupted
// without re-running the provider.
func (b *Bridge) Resume(ctx context.Context, runID session.RunID) (runtime.Handle, error) {
	return b.orch.Resume(ctx, runID)
}

// Run reads a run record.
func (b *Bridge) Run(ctx context.Context, runID session.RunID) (session.Run, error) {
	return b.store.GetRun(ctx, runID)
}

// Committed reads the bounded committed snapshot of a session.
func (b *Bridge) Committed(ctx context.Context, sessionID session.ID) (session.ObservationSnapshot, error) {
	return b.store.ReadObservationSnapshot(ctx, sessionID, ObservationLimits())
}

// AssistantText returns the committed assistant text of a run from a
// snapshot. ok is false when the message is absent or not finalized.
func AssistantText(snapshot session.ObservationSnapshot, runID session.RunID) (text string, ok bool) {
	for _, m := range snapshot.Messages {
		if m.RunID == runID && m.Role == session.RoleAssistant {
			return m.Text, m.Finalized
		}
	}
	return "", false
}

// HistoryWithinLimits reports whether a session may admit another turn
// under the bounded history policy (100 admitted turns, 256 KiB display
// text). It uses one bounded committed read and never loads provider
// payloads.
func (b *Bridge) HistoryWithinLimits(ctx context.Context, sessionID session.ID) (bool, error) {
	limits := session.ObservationLimits{MaxMessages: 2 * config.MaxHistoryTurns, MaxTools: 1, MaxParts: 4 * config.MaxHistoryTurns, MaxSnapshotBytes: 4 << 20, MaxTextBytes: config.MaxHistoryBytes}
	snap, err := b.store.ReadObservationSnapshot(ctx, sessionID, limits)
	if errors.Is(err, session.ErrObservationTooLarge) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !snap.Exists {
		return true, nil
	}
	if snap.OmittedOlderMessages {
		return false, nil
	}
	turns, bytes := 0, 0
	for _, m := range snap.Messages {
		if m.Role == session.RoleUser {
			turns++
		}
		bytes += len(m.Text)
	}
	return turns < config.MaxHistoryTurns && bytes <= config.MaxHistoryBytes, nil
}

// ProviderSessionID derives the opaque, stable provider session identity
// for a runtime session. Raw platform IDs never reach the provider.
func ProviderSessionID(runtimeSessionID string) string {
	sum := sha256.Sum256([]byte("eino-channels-provider-session|" + runtimeSessionID))
	return "ecs-" + hex.EncodeToString(sum[:16])
}

// FixedResolver accepts exactly the configured provider and model.
type FixedResolver struct {
	Streamer model.Streamer
}

// Resolve implements model.Resolver.
func (r FixedResolver) Resolve(_ context.Context, sel model.Selection, _ model.Runtime) (model.Resolved, error) {
	if sel.ProviderID != config.ProviderID || sel.ModelID != config.ModelID {
		return model.Resolved{}, model.Error{Code: "model_unsupported", Message: "unsupported model selection", Cause: model.ErrProviderUnavailable}
	}
	return model.Resolved{
		Provider: model.Provider{ID: config.ProviderID},
		Model:    model.Descriptor{ID: config.ModelID, ProviderID: config.ProviderID},
		Streamer: r.Streamer,
	}, nil
}

// Streamer wraps a provider streamer with the host's tool-free policy,
// request size bound, per-call provider session identity and Extra
// stripping (the OpenAI adapter attaches Extra that the runtime rejects
// without a provider-state codec).
type Streamer struct {
	inner model.Streamer
}

// NewStreamer wraps inner.
func NewStreamer(inner model.Streamer) Streamer { return Streamer{inner: inner} }

// StreamProvider implements model.Streamer.
func (s Streamer) StreamProvider(ctx context.Context, request model.Request) (*einoschema.StreamReader[model.StreamDelta], error) {
	if len(request.Tools) != 0 {
		return nil, model.Error{Code: "tools_unsupported", Message: ErrToolsNotAllowed.Error(), Cause: ErrToolsNotAllowed}
	}
	size := len(request.System)
	for _, m := range request.Messages {
		if m == nil {
			continue
		}
		size += len(m.Content)
		if size > config.MaxProviderReqBytes {
			return nil, model.Error{Code: "request_too_large", Message: ErrRequestTooLarge.Error(), Cause: ErrRequestTooLarge}
		}
	}
	if request.Identity.SessionID == "" {
		return nil, model.Error{Code: "session_missing", Message: "runtime session identity missing", Cause: model.ErrProviderUnavailable}
	}
	ctx = opencodeauth.WithSessionID(ctx, ProviderSessionID(request.Identity.SessionID))
	reader, err := s.inner.StreamProvider(ctx, request)
	if err != nil {
		return nil, err
	}
	return einoschema.StreamReaderWithConvert(reader, func(d model.StreamDelta) (model.StreamDelta, error) {
		if d.Message == nil {
			return d, nil
		}
		if len(d.Message.ToolCalls) != 0 {
			return model.StreamDelta{}, model.Error{Code: "unsupported_output", Message: ErrUnsupportedOutput.Error(), Cause: ErrUnsupportedOutput}
		}
		clone := *d.Message
		clone.Extra = nil
		d.Message = &clone
		return d, nil
	}), nil
}
