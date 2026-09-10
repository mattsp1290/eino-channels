// Package conversation is the shared, transport-neutral conversation
// service: durable ingestion, serialized per-route execution through the
// agent bridge, control handling, watch projection, ordered delivery and
// crash recovery. No Slack or Discord SDK type crosses this boundary.
package conversation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/runtime"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/state"
)

// Reserved control commands.
const (
	CommandStop = "!stop"
	CommandNew  = "!new"
	CommandHelp = "!help"
)

// Fixed user-facing texts.
const (
	NoticeHelp         = "I answer plain-text questions in DMs and when you mention me in an allowed channel. In shared threads, mention me on every message. Commands: !stop interrupts the current answer, !new starts a fresh conversation (thread creator only), !help shows this. Limits: prompts up to 16 KiB, 100 turns per conversation, answers up to 32 KiB. Attached files are not read."
	NoticeBusyNew      = "Finish or resolve pending work before starting a new conversation."
	NoticeNewStarted   = "Started a new conversation."
	NoticeStopped      = "Stopped."
	NoticeNothingToDo  = "Nothing is running."
	NoticeDenied       = "You are not allowed to control this conversation."
	NoticeOverflow     = "I am at capacity right now. Please try again in a moment."
	NoticeOversize     = "That message is too long. Please keep prompts under 16 KiB."
	NoticeHistoryLimit = "This conversation has reached its history limit. Use !new to start a fresh one."
	NoticeFiles        = "Note: attached files are not read; only the text was used."
	NoticeConflict     = "This message could not be processed. Please send it again as a new message."
	NoticeUnavailable  = "The service is temporarily unavailable. Please try again later."
	TextEmptyAnswer    = "(The model returned an empty response.)"
	TextFailed         = "Sorry, the model request failed. Please try again."
	TextStopped        = "\n\n(Stopped.)"
	TextStoppedEmpty   = "(Stopped before any response.)"
	TextOutputLimit    = "\n\n(Output limit reached.)"
	TextInterrupted    = "\n\n(This answer was interrupted by a service restart. You can continue the conversation.)"
	TextInterruptedNil = "(This answer was interrupted by a service restart before any text was produced. You can continue the conversation.)"
	TextUnavailable    = "(The response could not be displayed.)"
)

// ErrClosing is returned by Ingest during shutdown; adapters must not
// acknowledge the platform event so it is redelivered later.
var ErrClosing = errors.New("conversation service is shutting down")

// ErrKind classifies delivery failures.
type ErrKind int

// Delivery failure kinds.
const (
	KindDefinite    ErrKind = iota // definite failure, retry allowed
	KindAmbiguous                  // outcome unknown after a create
	KindRateLimited                // wait RetryAfter then retry
	KindPermanent                  // stop retrying, operator resolution
)

// DeliveryError is the classified adapter failure.
type DeliveryError struct {
	Kind       ErrKind
	RetryAfter time.Duration
	Err        error
}

// Error implements error with fixed safe text.
func (e *DeliveryError) Error() string {
	switch e.Kind {
	case KindAmbiguous:
		return "delivery outcome unknown"
	case KindRateLimited:
		return "delivery rate limited"
	case KindPermanent:
		return "delivery permanently failed"
	}
	return "delivery failed"
}

// Unwrap exposes the cause for logs (never for user-facing text).
func (e *DeliveryError) Unwrap() error { return e.Err }

// Deliverer is the platform delivery adapter. Destinations are immutable.
type Deliverer interface {
	Create(ctx context.Context, dest state.Destination, text, nonce string) (remoteID string, err error)
	Edit(ctx context.Context, dest state.Destination, remoteID, text string) error
	// Reconcile tries to identify an own message created with nonce.
	Reconcile(ctx context.Context, dest state.Destination, nonce string) (remoteID string, found bool, err error)
	// Allowed rechecks destination authorization before dispatch. A false
	// result is a denial; an error means the check could not be made now.
	Allowed(ctx context.Context, dest state.Destination) (bool, error)
	// Notify sends a transient, best-effort notice.
	Notify(ctx context.Context, dest state.Destination, text string) error
	// Chunks renders committed text into platform-safe ordered chunks.
	Chunks(text string) []string
	// Preview renders transient text into one platform-safe chunk.
	Preview(text string) string
}

// Options configure the service.
type Options struct {
	Limits     config.Limits
	Store      *state.Store
	Bridge     *agentbridge.Bridge
	Deliverers map[state.Platform]Deliverer
	Logger     *slog.Logger
	// DeliverySpacing is the minimum spacing between platform calls per
	// destination and the preview coalescing interval. Zero selects the
	// default (1.2 s). Tests may shorten it.
	DeliverySpacing time.Duration
}

// Service is the conversation service.
type Service struct {
	spacing    time.Duration
	limits     config.Limits
	st         *state.Store
	bridge     *agentbridge.Bridge
	deliverers map[state.Platform]Deliverer
	log        *slog.Logger

	ctx     context.Context
	cancel  context.CancelFunc
	loops   sync.WaitGroup // scheduler and notice sender
	runners sync.WaitGroup // route runners
	wake    chan struct{}
	sem     chan struct{}

	mu        sync.Mutex
	closing   bool
	active    map[string]struct{}
	parked    map[string]time.Time
	handles   map[string]runtime.Handle
	stopFlags map[string]bool   // keyed by run ID
	ingests   map[string]uint64 // per-route ingest counter; parking is conditional on it
	notices   chan notice

	limiterMu   sync.Mutex
	limiterLast map[state.Destination]time.Time
	cursor      string
}

type notice struct {
	dest state.Destination
	text string
}

// New builds a service. Start must be called before Ingest is used.
func New(opts Options) (*Service, error) {
	if opts.Store == nil || opts.Bridge == nil || len(opts.Deliverers) == 0 {
		return nil, errors.New("conversation: store, bridge and deliverers required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Limits.MaxRunningConversations <= 0 {
		opts.Limits = config.DefaultLimits()
	}
	if opts.DeliverySpacing <= 0 {
		opts.DeliverySpacing = config.PreviewCoalesceMillis * time.Millisecond
	}
	return &Service{
		spacing: opts.DeliverySpacing,
		limits:  opts.Limits, st: opts.Store, bridge: opts.Bridge, deliverers: opts.Deliverers, log: opts.Logger,
		wake: make(chan struct{}, 1), sem: make(chan struct{}, opts.Limits.MaxRunningConversations),
		active: map[string]struct{}{}, parked: map[string]time.Time{}, handles: map[string]runtime.Handle{}, stopFlags: map[string]bool{}, ingests: map[string]uint64{},
		notices: make(chan notice, 64), limiterLast: map[state.Destination]time.Time{},
	}, nil
}

// Start launches recovery, the scheduler and the notice sender.
func (s *Service) Start(ctx context.Context) {
	s.mu.Lock()
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()
	s.loops.Add(2)
	go s.noticeLoop()
	go s.scheduler()
	s.signal()
}

// Response tells the adapter what to do after ingestion.
type Response struct {
	Outcome state.Outcome
	// Notice is a transient message to send to the source destination, or "".
	Notice string
	// Item is the stored inbox row.
	Item state.Item
}

// Ingest durably records one normalized platform event. Adapters call it
// before acknowledging the platform delivery.
func (s *Service) Ingest(ctx context.Context, in state.Inbound) (Response, error) {
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return Response{}, ErrClosing
	}
	in.Kind, in.Content = classify(in.Content)
	if in.Kind == state.KindPrompt {
		if !utf8.ValidString(in.Content) || in.Content == "" {
			return Response{}, errors.New("conversation: prompt must be nonempty valid UTF-8")
		}
		if !in.Route.IsDM() {
			in.Content = "[" + in.ActorLabel + "] " + in.Content
		}
		if len(in.Content) > s.limits.MaxPromptBytes {
			in.RejectCode = state.CodeOversize
		}
	} else if in.RejectCode == "" {
		if code := s.authorizeControl(ctx, in); code != "" {
			in.RejectCode = code
		}
	}
	d, err := s.st.Ingest(ctx, in, state.Capacity{MaxQueuedPerRoute: s.limits.MaxQueuedPerConversation, MaxPendingGlobal: s.limits.MaxPendingInbox})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Response{}, err
		}
		s.log.Error("ingest failed", "platform", in.Route.Platform, "error", safeErr(err))
		return Response{}, fmt.Errorf("%w", state.ErrStorage)
	}
	resp := Response{Outcome: d.Outcome, Item: d.Item}
	switch d.Outcome {
	case state.OutcomeDuplicate:
		return resp, nil
	case state.OutcomeRejected:
		switch d.Item.ResultCode {
		case state.CodeOverflow:
			resp.Notice = NoticeOverflow
		case state.CodeOversize:
			resp.Notice = NoticeOversize
		case state.CodeBusy:
			resp.Notice = NoticeBusyNew
		case state.CodeDenied:
			resp.Notice = NoticeDenied
		}
		return resp, nil
	}
	switch in.Kind {
	case state.KindHelp:
		resp.Notice = NoticeHelp
	case state.KindNew:
		resp.Notice = NoticeNewStarted
	case state.KindStop:
		if d.StopNoop {
			resp.Notice = NoticeNothingToDo
		} else {
			s.interruptRoute(in.Route.Key(), d.Item.StopTargetRunID)
		}
	case state.KindPrompt:
		if in.FilesNotice {
			resp.Notice = NoticeFiles
		}
	}
	s.unpark(in.Route.Key())
	s.signal()
	return resp, nil
}

// classify separates reserved commands from conversation input. Only exact
// command text (after trimming) is a control; anything else is a prompt.
func classify(text string) (state.Kind, string) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case CommandStop:
		return state.KindStop, ""
	case CommandNew:
		return state.KindNew, ""
	case CommandHelp:
		return state.KindHelp, ""
	}
	return state.KindPrompt, strings.TrimSpace(text)
}

// authorizeControl returns a reject code when the actor may not issue the
// control on this route.
func (s *Service) authorizeControl(ctx context.Context, in state.Inbound) string {
	if in.Kind == state.KindHelp {
		return ""
	}
	if in.Route.IsDM() {
		if in.Route.DMActor != in.Actor {
			return state.CodeDenied
		}
		return ""
	}
	conv, err := s.st.GetConversation(ctx, in.Route)
	if errors.Is(err, state.ErrNotFound) {
		return "" // first actor becomes the creator
	}
	if err != nil {
		return state.CodeDenied
	}
	if conv.CreatorActor == in.Actor {
		return ""
	}
	if in.Kind == state.KindStop {
		if active, err := s.st.ActiveItem(ctx, in.Route.Key()); err == nil && active.Actor == in.Actor {
			return ""
		}
	}
	return state.CodeDenied
}

func (s *Service) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Notify queues a transient notice; it never blocks ingestion.
func (s *Service) Notify(dest state.Destination, text string) {
	select {
	case s.notices <- notice{dest: dest, text: text}:
	default:
		s.log.Warn("notice dropped: queue full", "platform", dest.Platform)
	}
}

func (s *Service) noticeLoop() {
	defer s.loops.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case n := <-s.notices:
			d, ok := s.deliverers[n.dest.Platform]
			if !ok {
				continue
			}
			ctx, cancel := context.WithTimeout(s.ctx, s.platformTimeout())
			if allowed, err := d.Allowed(ctx, n.dest); err != nil || !allowed {
				if err != nil {
					s.log.Warn("notice dropped: destination check failed", "platform", n.dest.Platform, "error", safeErr(err))
				}
				cancel()
				continue
			}
			if err := s.throttle(ctx, n.dest); err != nil {
				cancel()
				continue
			}
			if err := d.Notify(ctx, n.dest, n.text); err != nil {
				s.log.Warn("notice failed", "platform", n.dest.Platform, "error", safeErr(err))
			}
			cancel()
		}
	}
}

func (s *Service) platformTimeout() time.Duration {
	return time.Duration(s.limits.PlatformCallSeconds) * time.Second
}

func (s *Service) turnTimeout() time.Duration {
	return time.Duration(s.limits.ModelTurnSeconds) * time.Second
}

// interruptRoute interrupts the owned handle of a route when it matches the
// frozen target (or when no target run is known yet).
func (s *Service) interruptRoute(routeKey, targetRunID string) {
	s.mu.Lock()
	h := s.handles[routeKey]
	s.mu.Unlock()
	if h == nil {
		return
	}
	if targetRunID != "" && string(h.RunID()) != targetRunID {
		return
	}
	s.setStopFlag(string(h.RunID()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.Interrupt(ctx, "user stop")
}

// Stop flags are keyed by run ID so a stop that arrives around the
// terminal boundary of run N can never relabel run N+1.
func (s *Service) setStopFlag(runID string) {
	s.mu.Lock()
	s.stopFlags[runID] = true
	s.mu.Unlock()
}

func (s *Service) consumeStopFlag(runID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.stopFlags[runID]
	delete(s.stopFlags, runID)
	return v
}

// Shutdown stops ingestion, interrupts owned runs and waits for settlement
// until the deadline. It returns an error when work did not settle in time;
// recovery records remain intact either way.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	handles := make([]runtime.Handle, 0, len(s.handles))
	for _, h := range s.handles {
		handles = append(handles, h)
	}
	s.mu.Unlock()
	for _, h := range handles {
		ictx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = h.Interrupt(ictx, "service shutdown")
		cancel()
	}
	if s.cancel == nil {
		return nil
	}
	done := make(chan struct{})
	go func() { s.runners.Wait(); close(done) }()
	s.signal()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = errors.New("shutdown deadline exceeded; recovery records retained")
	}
	s.cancel()
	// After cancellation every runner path observes s.ctx and returns
	// promptly; bound the wait anyway so a stuck platform call cannot hold
	// the process past its exit deadline.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.log.Error("runners did not stop after cancellation; exiting with recovery records intact")
	}
	s.loops.Wait()
	return err
}

// safeErr renders an error for logs without provider or platform bodies.
func safeErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > 200 {
		cut := 200
		for cut > 0 && !utf8.RuneStart(msg[cut]) {
			cut--
		}
		msg = msg[:cut]
	}
	return msg
}
