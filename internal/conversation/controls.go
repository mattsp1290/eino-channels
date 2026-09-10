package conversation

import (
	"errors"
	"time"

	"github.com/mattsp1290/eino-agent/session"

	"github.com/mattsp1290/eino-channels/internal/state"
)

// settleStop replays a frozen stop control idempotently: cancel queued
// prompts up to the cutoff, settle the frozen target, then mark complete.
func (s *Service) settleStop(conv state.Conversation, ctl state.Item) workOutcome {
	key := conv.Route.Key()
	if err := s.st.CancelQueuedUpTo(s.ctx, key, ctl.StopCutoffSeq); err != nil {
		return workPark
	}
	if ctl.StopTargetInboxID != 0 {
		target, err := s.st.GetItem(s.ctx, ctl.StopTargetInboxID)
		if err != nil && !isNotFound(err) {
			return workPark
		}
		if err == nil {
			switch target.State {
			case state.StateAdmitting:
				// Resolve the keyed receipt before applying interruption.
				rec, err := s.bridge.Lookup(s.ctx, session.ID(conv.RuntimeSessionID), target.AdmissionKey)
				switch {
				case err == nil:
					if err := s.st.MarkAdmitted(s.ctx, target.ID, string(rec.Receipt.RunID), string(rec.Receipt.UserMessageID), string(rec.Receipt.AssistantMessageID)); err != nil {
						return workPark
					}
					target.RunID = string(rec.Receipt.RunID)
					target.State = state.StateAdmitted
				case isNotFound(err):
					// Never started: cancel without starting it.
					if err := s.st.Transition(s.ctx, target.ID, state.StateAdmitting, state.StateCanceled, state.CodeCanceled); err != nil && !errors.Is(err, state.ErrConflict) {
						return workPark
					}
				default:
					return workPark // unresolved outcome stays pending
				}
			}
			if target.State == state.StateAdmitted {
				if outcome := s.settleRun(target); outcome != workDone {
					return outcome
				}
			}
		}
	}
	err := s.st.Transition(s.ctx, ctl.ID, state.StatePending, state.StateComplete, "")
	switch {
	case err == nil:
		s.Notify(conv.Route.Destination(), NoticeStopped)
	case errors.Is(err, state.ErrConflict):
		// Already completed by an earlier replay: no second notice.
	default:
		return workPark
	}
	return workDone
}

// settleRun interrupts an admitted target run (owned or via lease
// recovery) and leaves it to the normal prompt path to finalize. It only
// waits until the run is terminal.
func (s *Service) settleRun(target state.Item) workOutcome {
	runID := session.RunID(target.RunID)
	s.interruptRoute(target.RouteKey, target.RunID)
	deadline := time.Now().Add(15 * time.Second)
	for {
		run, err := s.bridge.Run(s.ctx, runID)
		if err != nil {
			return workPark
		}
		if run.Terminal() {
			return workDone
		}
		if time.Now().After(run.LeaseUntil) {
			h, err := s.bridge.Resume(s.ctx, runID)
			if err == nil {
				select {
				case <-h.Done():
				case <-s.ctx.Done():
					return workStop
				}
				continue
			}
		}
		if time.Now().After(deadline) {
			return workPark
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-s.ctx.Done():
			return workStop
		}
	}
}
