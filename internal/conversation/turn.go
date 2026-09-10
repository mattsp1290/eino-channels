package conversation

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/state"
)

const (
	maxAdmissionAttempts = 3
	maxWatchResyncs      = 3
	finalRevision        = 1
)

func isNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound) || errors.Is(err, session.ErrNotFound)
}

func (s *Service) turn(conv state.Conversation, item state.Item) agentbridge.Turn {
	return agentbridge.Turn{SessionID: session.ID(conv.RuntimeSessionID), AdmissionKey: item.AdmissionKey, Content: item.Content, WorkspaceID: conv.Route.Key()}
}

// runPrompt executes or recovers one prompt item to a terminal, planned
// delivery. It acquires a model worker slot for the duration.
func (s *Service) runPrompt(conv *state.Conversation, item state.Item) workOutcome {
	select {
	case s.sem <- struct{}{}:
	case <-s.ctx.Done():
		return workStop
	}
	defer func() { <-s.sem }()
	dest := conv.Route.Destination()
	deliverer := s.deliverers[dest.Platform]
	if deliverer == nil {
		s.log.Error("no deliverer for platform", "platform", dest.Platform)
		return workPark
	}
	if item.Generation != conv.Generation {
		// Accepted before a rotation could only happen through a bug; never run it.
		if err := s.st.Transition(s.ctx, item.ID, "", state.StateCanceled, state.CodeCanceled); err != nil {
			s.log.Error("cancel stale-generation item", "inbox", item.ID, "error", safeErr(err))
			return workPark
		}
		return workDone
	}
	sessionID := session.ID(conv.RuntimeSessionID)

	// Phase 1: obtain a receipt (new admission or recovery).
	var receipt session.AdmissionReceipt
	var handle runtime.Handle
	cancelRun := func() {}
	defer func() { cancelRun() }()
	switch item.State {
	case state.StateQueued:
		ok, err := s.bridge.HistoryWithinLimits(s.ctx, sessionID)
		if err != nil {
			s.log.Warn("history check failed", "error", safeErr(err))
			return workPark
		}
		if !ok {
			_ = s.st.Transition(s.ctx, item.ID, state.StateQueued, state.StateRejected, state.CodeHistoryLimit)
			s.Notify(dest, NoticeHistoryLimit)
			return workDone
		}
		if err := s.st.Transition(s.ctx, item.ID, state.StateQueued, state.StateAdmitting, ""); err != nil {
			s.log.Warn("admitting transition failed", "error", safeErr(err))
			return workPark
		}
		item.State = state.StateAdmitting
		fallthrough
	case state.StateAdmitting:
		var outcome workOutcome
		receipt, handle, cancelRun, outcome = s.admit(*conv, item, dest)
		if outcome != workDone {
			return outcome
		}
		item.RunID = string(receipt.RunID)
	case state.StateAdmitted:
		receipt = session.AdmissionReceipt{SessionID: sessionID, Key: item.AdmissionKey, RunID: session.RunID(item.RunID), UserMessageID: session.MessageID(item.ReceiptUserMsg), AssistantMessageID: session.MessageID(item.ReceiptAssistant)}
	default:
		return workDone
	}

	// Phase 2: own or recover the run until terminal.
	sub := s.subscribe(sessionID)
	if handle == nil {
		var outcome workOutcome
		handle, outcome = s.recoverHandle(receipt.RunID)
		if outcome != workDone {
			if sub != nil {
				sub.Close()
			}
			return outcome
		}
	}
	s.mu.Lock()
	s.handles[conv.Route.Key()] = handle
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.handles, conv.Route.Key())
		s.mu.Unlock()
	}()

	// Replay a stop that was frozen against this item before we owned it.
	if stops, err := s.st.PendingStops(s.ctx, conv.Route.Key()); err == nil {
		for _, st := range stops {
			if st.StopTargetInboxID == item.ID || st.StopTargetRunID == item.RunID {
				s.setStopFlag(item.RunID)
				ictx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
				_ = handle.Interrupt(ictx, "user stop")
				cancel()
			}
		}
	}

	proj := newProjection(s, *conv, item, deliverer, sub, handle)
	result := proj.wait()
	proj.stop()
	if result.Status == session.RunFailed {
		s.log.Warn("run failed", "inbox", item.ID, "code", safeRunError(result.Error))
	}
	userStop := s.consumeStopFlag(item.RunID)

	// Phase 3: read committed text and plan deliveries.
	// ok (finalized) matters only for completed runs; an interrupted run's
	// placeholder is never finalized, so its committed text is empty.
	text, ok, unavailable := s.committedText(sessionID, receipt.RunID)
	final, code := composeFinal(result, text, ok, unavailable, proj.limitHit(), userStop, proj.liveText())
	plans := make([]state.DeliveryPlan, 0, 4)
	for i, chunk := range deliverer.Chunks(final) {
		plans = append(plans, state.DeliveryPlan{ChunkIndex: i, Text: chunk, Revision: finalRevision})
	}
	if err := s.st.MarkTerminal(s.ctx, item.ID, string(result.Status), code, plans); err != nil {
		s.log.Error("mark terminal failed", "error", safeErr(err))
		return workPark
	}
	return workDone
}

// admit performs the keyed Start with authoritative recovery on failure.
func (s *Service) admit(conv state.Conversation, item state.Item, dest state.Destination) (session.AdmissionReceipt, runtime.Handle, context.CancelFunc, workOutcome) {
	sessionID := session.ID(conv.RuntimeSessionID)
	noop := context.CancelFunc(func() {})
	for {
		// Authoritative lookup first: a receipt may already exist from a
		// previous attempt that died before persisting it.
		rec, err := s.bridge.Lookup(s.ctx, sessionID, item.AdmissionKey)
		switch {
		case err == nil:
			if err := s.st.MarkAdmitted(s.ctx, item.ID, string(rec.Receipt.RunID), string(rec.Receipt.UserMessageID), string(rec.Receipt.AssistantMessageID)); err != nil {
				return session.AdmissionReceipt{}, nil, noop, workPark
			}
			return rec.Receipt, nil, noop, workDone
		case !isNotFound(err):
			s.log.Warn("admission lookup unresolved", "error", safeErr(err))
			return session.AdmissionReceipt{}, nil, noop, workPark
		}
		attempts, err := s.st.IncrementAttempts(s.ctx, item.ID)
		if err != nil {
			return session.AdmissionReceipt{}, nil, noop, workPark
		}
		if attempts > maxAdmissionAttempts {
			_ = s.st.Transition(s.ctx, item.ID, state.StateAdmitting, state.StateRejected, state.CodeUnavailable)
			s.Notify(dest, NoticeUnavailable)
			return session.AdmissionReceipt{}, nil, noop, workDone
		}
		runCtx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), s.turnTimeout())
		res, err := s.bridge.Submit(runCtx, s.turn(conv, item))
		if err != nil {
			cancel()
			switch {
			case errors.Is(err, session.ErrAdmissionConflict):
				s.log.Error("admission fingerprint conflict; operator diagnostic: frozen payload differs from the committed receipt", "inbox", item.ID)
				_ = s.st.Transition(s.ctx, item.ID, state.StateAdmitting, state.StateRejected, state.CodeConflict)
				s.Notify(dest, NoticeConflict)
				return session.AdmissionReceipt{}, nil, noop, workDone
			case errors.Is(err, session.ErrAdmissionInvalid):
				_ = s.st.Transition(s.ctx, item.ID, state.StateAdmitting, state.StateRejected, state.CodeConflict)
				s.Notify(dest, NoticeConflict)
				return session.AdmissionReceipt{}, nil, noop, workDone
			case errors.Is(err, session.ErrSessionBusy):
				// Another run owns the session (an abandoned lease). Let it settle.
				s.log.Warn("session busy at admission; waiting for lease recovery")
				time.Sleep(time.Duration(config.AgentLeaseSeconds) * time.Second)
				continue
			}
			s.log.Warn("admission attempt failed", "error", safeErr(err))
			// Back off before the authoritative lookup and retry.
			select {
			case <-time.After(time.Duration(attempts) * time.Second):
			case <-s.ctx.Done():
				return session.AdmissionReceipt{}, nil, noop, workStop
			}
			continue
		}
		if err := s.st.MarkAdmitted(s.ctx, item.ID, string(res.Receipt.RunID), string(res.Receipt.UserMessageID), string(res.Receipt.AssistantMessageID)); err != nil {
			s.log.Error("receipt persistence failed after admission", "error", safeErr(err))
			// The run may be executing; recovery will find the receipt by key.
			// Wait for it to settle, but never past shutdown.
			if res.Handle != nil {
				select {
				case <-res.Handle.Done():
				case <-s.ctx.Done():
					ictx, icancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = res.Handle.Interrupt(ictx, "service shutdown")
					icancel()
				}
			}
			cancel()
			return session.AdmissionReceipt{}, nil, noop, workPark
		}
		if res.Handle == nil {
			cancel()
			return res.Receipt, nil, noop, workDone
		}
		// The caller is the single Done consumer; cancel releases the turn deadline afterwards.
		return res.Receipt, res.Handle, cancel, workDone
	}
}

// recoverHandle obtains a control handle for a run this process does not
// own: terminal runs return immediately; abandoned runs are resumed after
// their lease expires (settling as interrupted with no tools).
func (s *Service) recoverHandle(runID session.RunID) (runtime.Handle, workOutcome) {
	for {
		run, err := s.bridge.Run(s.ctx, runID)
		if err != nil {
			s.log.Warn("run lookup failed during recovery", "error", safeErr(err))
			return nil, workPark
		}
		if run.Terminal() {
			return terminalHandle(run), workDone
		}
		if wait := time.Until(run.LeaseUntil); wait > 0 {
			select {
			case <-time.After(wait + 100*time.Millisecond):
			case <-s.ctx.Done():
				return nil, workStop
			}
			continue
		}
		h, err := s.bridge.Resume(s.ctx, runID)
		if errors.Is(err, session.ErrSessionBusy) {
			select {
			case <-time.After(time.Second):
			case <-s.ctx.Done():
				return nil, workStop
			}
			continue
		}
		if err != nil {
			s.log.Warn("resume failed", "error", safeErr(err))
			return nil, workPark
		}
		return h, workDone
	}
}

type staticHandle struct {
	runID session.RunID
	done  chan runtime.Result
}

func terminalHandle(run session.Run) runtime.Handle {
	done := make(chan runtime.Result, 1)
	done <- runtime.Result{RunID: run.ID, Status: run.Status, Interrupted: run.Status == session.RunInterrupted}
	close(done)
	return &staticHandle{runID: run.ID, done: done}
}

func (h *staticHandle) RunID() session.RunID                    { return h.runID }
func (h *staticHandle) Done() <-chan runtime.Result             { return h.done }
func (h *staticHandle) Interrupt(context.Context, string) error { return nil }

func (s *Service) subscribe(sessionID session.ID) *watch.Subscription {
	sub, err := s.bridge.Watch(s.ctx, sessionID)
	if err != nil {
		s.log.Warn("watch unavailable; previews disabled for this turn", "error", safeErr(err))
		return nil
	}
	return sub
}

// committedText reads the committed assistant text of a run. unavailable
// reports an observation overflow that must not be rendered as truncated.
func (s *Service) committedText(sessionID session.ID, runID session.RunID) (text string, ok, unavailable bool) {
	snap, err := s.bridge.Committed(s.ctx, sessionID)
	if errors.Is(err, session.ErrObservationTooLarge) {
		return "", false, true
	}
	if err != nil {
		s.log.Warn("committed read failed", "error", safeErr(err))
		return "", false, true
	}
	text, ok = agentbridge.AssistantText(snap, runID)
	return text, ok, false
}

// composeFinal renders the delivered text from the terminal result and the
// committed assistant text. A completed answer only ever comes from
// committed text. An interrupted answer may show the in-process live prefix,
// explicitly labeled as stopped or interrupted; after a restart no live
// prefix exists and only the notice is delivered.
func composeFinal(result runtime.Result, text string, ok, unavailable, limitHit, userStop bool, live string) (string, string) {
	if unavailable {
		return TextUnavailable, state.CodeUnavailable
	}
	text = strings.TrimSpace(text)
	switch result.Status {
	case session.RunCompleted:
		if text == "" || !ok {
			return TextEmptyAnswer, state.CodeCompleted
		}
		if len(text) > config.MaxOutputBytes {
			// The model finished before the streaming cap could cancel it:
			// only the accepted prefix is delivered, explicitly labeled.
			return truncateUTF8(text, config.MaxOutputBytes) + TextOutputLimit, state.CodeOutputLimit
		}
		return text, state.CodeCompleted
	case session.RunInterrupted:
		partial := text
		if partial == "" {
			partial = strings.TrimSpace(live)
		}
		partial = truncateUTF8(partial, config.MaxOutputBytes)
		switch {
		case limitHit:
			return partial + TextOutputLimit, state.CodeOutputLimit
		case userStop && partial != "":
			return partial + TextStopped, state.CodeInterrupted
		case userStop:
			return TextStoppedEmpty, state.CodeInterrupted
		case partial != "":
			return partial + TextInterrupted, state.CodeInterrupted
		}
		return TextInterruptedNil, state.CodeInterrupted
	}
	return TextFailed, state.CodeFailed
}

// truncateUTF8 cuts s to at most n bytes on a rune boundary.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// safeRunError reduces a run error to a classification code for logs.
func safeRunError(err error) string {
	if err == nil {
		return ""
	}
	var me model.Error
	if errors.As(err, &me) && me.Code != "" {
		return me.Code
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "canceled"
	}
	return "provider_error"
}

// projection consumes watch updates for one run, coalesces previews and
// enforces the output cap. It owns the single Done consumer.
type projection struct {
	s         *Service
	conv      state.Conversation
	item      state.Item
	deliverer Deliverer
	sub       *watch.Subscription
	handle    runtime.Handle

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	live     string
	lastSeen string // last non-empty observed prefix, kept across LiveUnavailable
	dirty    bool
	limit    bool
	resyncs  int
	previewR *state.Delivery
	lastEdit time.Time
}

func newProjection(s *Service, conv state.Conversation, item state.Item, d Deliverer, sub *watch.Subscription, h runtime.Handle) *projection {
	p := &projection{s: s, conv: conv, item: item, deliverer: d, sub: sub, handle: h}
	p.ctx, p.cancel = context.WithCancel(s.ctx)
	if sub != nil {
		p.wg.Add(2)
		go p.consume()
		go p.flushLoop()
	}
	return p
}

func (p *projection) wait() runtime.Result {
	select {
	case r := <-p.handle.Done():
		return r
	case <-p.s.ctx.Done():
		// Shutdown: give the interrupt a bounded chance to settle.
		select {
		case r := <-p.handle.Done():
			return r
		case <-time.After(5 * time.Second):
			return runtime.Result{RunID: p.handle.RunID(), Status: session.RunInterrupted}
		}
	}
}

func (p *projection) stop() {
	p.cancel()
	p.mu.Lock()
	sub := p.sub
	p.mu.Unlock()
	if sub != nil {
		sub.Close()
	}
	p.wg.Wait()
	// A resync may have swapped the subscription after the close above.
	p.mu.Lock()
	if p.sub != nil && p.sub != sub {
		p.sub.Close()
	}
	p.mu.Unlock()
}

func (p *projection) limitHit() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.limit
}

// liveText returns the last observed transient prefix of this run.
func (p *projection) liveText() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastSeen
}

func (p *projection) consume() {
	defer p.wg.Done()
	runID := p.handle.RunID()
	for {
		p.mu.Lock()
		sub := p.sub
		p.mu.Unlock()
		u, err := sub.Next(p.ctx)
		if err != nil {
			if errors.Is(err, watch.ErrResyncRequired) && p.resyncs < maxWatchResyncs {
				p.resyncs++
				select {
				case <-time.After(time.Duration(p.resyncs) * 200 * time.Millisecond):
				case <-p.ctx.Done():
					return
				}
				fresh, werr := p.s.bridge.Watch(p.ctx, session.ID(p.conv.RuntimeSessionID))
				if werr == nil {
					p.mu.Lock()
					old := p.sub
					p.sub = fresh
					p.mu.Unlock()
					old.Close()
					continue
				}
			}
			return
		}
		switch u.Kind {
		case watch.Live:
			if u.Live.Identity.RunID != runID {
				continue
			}
			p.setLive(u.Live.Text)
		case watch.LiveUnavailable:
			p.setLive("")
		case watch.Durable:
			for _, r := range u.Snapshot.Runs {
				if r.ID == runID && r.Terminal() {
					return
				}
			}
			if text, ok := agentbridge.AssistantText(u.Snapshot, runID); ok {
				p.setLive(text)
			}
		}
	}
}

func (p *projection) setLive(text string) {
	p.mu.Lock()
	if len(text) > config.MaxOutputBytes && !p.limit {
		p.limit = true
		p.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.handle.Interrupt(ctx, "output limit")
		cancel()
		return
	}
	if text != p.live {
		p.live, p.dirty = text, true
	}
	if text != "" {
		p.lastSeen = text
	}
	p.mu.Unlock()
}

// flushLoop creates the status message once and edits it with coalesced
// previews at most once per coalescing interval.
func (p *projection) flushLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.s.spacing)
	defer ticker.Stop()
	// Create the placeholder only when this run is next in the lane.
	if !p.ensurePreviewRow() {
		return
	}
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
		}
		p.mu.Lock()
		text, dirty := p.live, p.dirty
		p.dirty = false
		row := p.previewR
		p.mu.Unlock()
		if !dirty || row == nil || row.RemoteID == "" {
			continue
		}
		if err := p.s.throttle(p.ctx, row.Destination); err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(p.ctx, p.s.platformTimeout())
		err := p.deliverer.Edit(ctx, row.Destination, row.RemoteID, p.deliverer.Preview(text))
		cancel()
		if err != nil {
			var de *DeliveryError
			if errors.As(err, &de) && de.Kind == KindRateLimited && de.RetryAfter > 0 {
				select {
				case <-time.After(min(de.RetryAfter, 30*time.Second)):
				case <-p.ctx.Done():
					return
				}
			}
		}
	}
}

// ensurePreviewRow plans and creates the chunk-0 status message when the
// delivery lane is clear up to this run. It returns false when previews
// must be skipped for this turn.
func (p *projection) ensurePreviewRow() bool {
	row, err := p.s.st.PlanPreview(p.ctx, p.item, p.deliverer.Preview(""))
	if err != nil {
		return false
	}
	next, blocked, err := p.s.st.NextDelivery(p.ctx, p.conv.Route.Key())
	if err != nil || blocked || next.ID != row.ID {
		return false
	}
	if row.RemoteID == "" {
		// A create in flight must not be canceled by run completion: that is
		// exactly the ambiguous case. Persistence after a create is never canceled.
		ok, _ := p.s.createDelivery(context.WithoutCancel(p.ctx), p.deliverer, row)
		if !ok {
			return false
		}
		created, err := p.s.st.GetDelivery(p.ctx, row.ID)
		if err != nil || created.RemoteID == "" {
			return false
		}
		row = created
	}
	p.mu.Lock()
	p.previewR = &row
	p.mu.Unlock()
	return true
}
