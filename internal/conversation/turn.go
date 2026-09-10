package conversation

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/mattsp1290/eino-agent/model"
	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/redact"
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
			s.log.Error("cancel stale-generation item", "inbox", item.ID, "error", redact.Err(err))
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
			s.log.Warn("history check failed", "error", redact.Err(err))
			return workPark
		}
		if !ok {
			_ = s.st.Transition(s.ctx, item.ID, state.StateQueued, state.StateRejected, state.CodeHistoryLimit)
			s.Notify(conv.Route, NoticeHistoryLimit)
			return workDone
		}
		if err := s.st.Transition(s.ctx, item.ID, state.StateQueued, state.StateAdmitting, ""); err != nil {
			s.log.Warn("admitting transition failed", "error", redact.Err(err))
			return workPark
		}
		item.State = state.StateAdmitting
		fallthrough
	case state.StateAdmitting:
		var outcome workOutcome
		receipt, handle, cancelRun, outcome = s.admit(*conv, item)
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
	final, code := composeFinal(outcome{Result: result, Committed: text, Finalized: ok, Unavailable: unavailable, LimitHit: proj.limitHit(), UserStop: userStop, Live: proj.liveText()})
	plans := make([]state.DeliveryPlan, 0, 4)
	for i, chunk := range deliverer.Chunks(final) {
		plans = append(plans, state.DeliveryPlan{ChunkIndex: i, Text: chunk, Revision: finalRevision})
	}
	if err := s.st.MarkTerminal(s.ctx, item.ID, string(result.Status), code, plans); err != nil {
		s.log.Error("mark terminal failed", "error", redact.Err(err))
		return workPark
	}
	return workDone
}

// admit performs the keyed Start with authoritative recovery on failure.
func (s *Service) admit(conv state.Conversation, item state.Item) (session.AdmissionReceipt, runtime.Handle, context.CancelFunc, workOutcome) {
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
			s.log.Warn("admission lookup unresolved", "error", redact.Err(err))
			return session.AdmissionReceipt{}, nil, noop, workPark
		}
		attempts, err := s.st.IncrementAttempts(s.ctx, item.ID)
		if err != nil {
			return session.AdmissionReceipt{}, nil, noop, workPark
		}
		if attempts > maxAdmissionAttempts {
			_ = s.st.Transition(s.ctx, item.ID, state.StateAdmitting, state.StateRejected, state.CodeUnavailable)
			s.Notify(conv.Route, NoticeUnavailable)
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
				s.Notify(conv.Route, NoticeConflict)
				return session.AdmissionReceipt{}, nil, noop, workDone
			case errors.Is(err, session.ErrAdmissionInvalid), errors.Is(err, session.ErrAdmissionUnknown):
				// Unknown covers a receipt written by a different fingerprint
				// version: retrying cannot succeed, so reject with the same
				// safe notice as a conflict.
				_ = s.st.Transition(s.ctx, item.ID, state.StateAdmitting, state.StateRejected, state.CodeConflict)
				s.Notify(conv.Route, NoticeConflict)
				return session.AdmissionReceipt{}, nil, noop, workDone
			case errors.Is(err, session.ErrSessionBusy):
				// Another run owns the session (an abandoned lease). Let it settle.
				s.log.Warn("session busy at admission; waiting for lease recovery")
				select {
				case <-time.After(time.Duration(config.AgentLeaseSeconds) * time.Second):
				case <-s.ctx.Done():
					return session.AdmissionReceipt{}, nil, noop, workStop
				}
				continue
			}
			s.log.Warn("admission attempt failed", "error", redact.Err(err))
			// Back off before the authoritative lookup and retry.
			select {
			case <-time.After(time.Duration(attempts) * time.Second):
			case <-s.ctx.Done():
				return session.AdmissionReceipt{}, nil, noop, workStop
			}
			continue
		}
		if err := s.st.MarkAdmitted(s.ctx, item.ID, string(res.Receipt.RunID), string(res.Receipt.UserMessageID), string(res.Receipt.AssistantMessageID)); err != nil {
			s.log.Error("receipt persistence failed after admission", "error", redact.Err(err))
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
			s.log.Warn("run lookup failed during recovery", "error", redact.Err(err))
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
			s.log.Warn("resume failed", "error", redact.Err(err))
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
		s.log.Warn("watch unavailable; previews disabled for this turn", "error", redact.Err(err))
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
		s.log.Warn("committed read failed", "error", redact.Err(err))
		return "", false, true
	}
	text, ok = agentbridge.AssistantText(snap, runID)
	return text, ok, false
}

// outcome is everything composeFinal needs to render a terminal turn.
type outcome struct {
	Result      runtime.Result
	Committed   string // committed assistant text, if any
	Finalized   bool   // the committed message is finalized
	Unavailable bool   // the committed snapshot could not be read
	LimitHit    bool   // the streaming output cap cancelled the run
	UserStop    bool   // a user stop interrupted the run
	Live        string // last in-process live prefix
}

// composeFinal renders the delivered text and result code for a terminal
// turn. A completed answer only ever comes from committed text. An
// interrupted answer may show the in-process live prefix, explicitly
// labeled as stopped or interrupted; after a restart no live prefix exists
// and only the notice is delivered.
func composeFinal(o outcome) (text, code string) {
	if o.Unavailable {
		return TextUnavailable, state.CodeUnavailable
	}
	text = strings.TrimSpace(o.Committed)
	result, ok, limitHit, userStop, live := o.Result, o.Finalized, o.LimitHit, o.UserStop, o.Live
	switch result.Status {
	case session.RunCompleted:
		if text == "" || !ok {
			return TextEmptyAnswer, state.CodeCompleted
		}
		if len(text) > config.MaxOutputBytes {
			// The model finished before the streaming cap could cancel it:
			// only the accepted prefix is delivered, explicitly labeled.
			return redact.TruncateUTF8(text, config.MaxOutputBytes) + TextOutputLimit, state.CodeOutputLimit
		}
		return text, state.CodeCompleted
	case session.RunInterrupted:
		partial := text
		if partial == "" {
			partial = strings.TrimSpace(live)
		}
		partial = redact.TruncateUTF8(partial, config.MaxOutputBytes)
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
