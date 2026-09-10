package conversation

import (
	"context"
	"errors"
	"time"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/state"
)

const limiterPruneAge = 10 * time.Minute

// throttle enforces the minimum spacing between platform calls per
// destination. It returns the context error when the wait is cut short so
// the caller skips the call instead of sending with a dead context.
func (s *Service) throttle(ctx context.Context, dest state.Destination) error {
	s.limiterMu.Lock()
	now := time.Now()
	if len(s.limiterLast) > 1024 {
		for k, v := range s.limiterLast {
			if now.Sub(v) > limiterPruneAge {
				delete(s.limiterLast, k)
			}
		}
	}
	last := s.limiterLast[dest]
	wait := time.Until(last.Add(s.spacing))
	if wait > 0 {
		s.limiterLast[dest] = last.Add(s.spacing)
	} else {
		s.limiterLast[dest] = now
	}
	s.limiterMu.Unlock()
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// backoff returns 2s, 4s, ... capped at two minutes. attempts counts the
// attempts made so far, including the one that just failed.
func backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := 2 * time.Second << uint(min(attempts-1, 10))
	return min(d, 2*time.Minute)
}

func exhausted(row state.Delivery) bool {
	return row.Attempts >= config.DeliveryMaxAttempts || !row.FirstAttemptAt.IsZero() && time.Since(row.FirstAttemptAt) > config.DeliveryWindowMinutes*time.Minute
}

func (s *Service) recordf(err error, msg string, id int64) {
	if err != nil {
		s.log.Error(msg, "delivery", id, "error", safeErr(err))
	}
}

// persistCreated records a successful create. A create that returned a
// remote ID must never be repeated, so the write is retried without
// cancellation before giving up.
func (s *Service) persistCreated(id int64, remoteID string, revision int64) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = s.st.MarkCreated(ctx, id, remoteID, revision)
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	return err
}

// createDelivery persists an intent, performs one create, and records the
// outcome. ok is false when the row cannot proceed now; progressed reports
// whether durable state changed (the lane loop parks when it did not).
func (s *Service) createDelivery(ctx context.Context, d Deliverer, row state.Delivery) (ok, progressed bool) {
	if exhausted(row) {
		s.recordf(s.st.MarkFailed(ctx, row.ID, "automated attempts exhausted"), "mark failed", row.ID)
		return false, true
	}
	if row.Op == state.OpCreateIntent && row.Status == state.DeliveryPending {
		// An intent without a recorded outcome: a previous process may have
		// created the message. Never create again blindly.
		s.recordf(s.st.MarkAmbiguous(ctx, row.ID), "mark ambiguous", row.ID)
		s.reconcile(ctx, d, row)
		return false, true
	}
	intent, err := s.st.MarkCreateIntent(ctx, row.ID)
	if err != nil {
		s.recordf(err, "mark create intent", row.ID)
		return false, false
	}
	if !intent.HasDesiredText {
		s.recordf(s.st.MarkEdited(ctx, intent.ID, intent.DesiredRevision), "acknowledge empty payload", row.ID)
		return false, true
	}
	if err := s.throttle(ctx, intent.Destination); err != nil {
		return false, false
	}
	cctx, cancel := context.WithTimeout(ctx, s.platformTimeout())
	remoteID, err := d.Create(cctx, intent.Destination, intent.DesiredText, intent.Nonce)
	cancel()
	if err == nil {
		if err := s.persistCreated(intent.ID, remoteID, intent.DesiredRevision); err != nil {
			s.log.Error("create succeeded but could not be recorded; row left as intent for ambiguity handling", "delivery", intent.ID, "error", safeErr(err))
			return false, false
		}
		return true, true
	}
	var de *DeliveryError
	if !errors.As(err, &de) {
		de = &DeliveryError{Kind: KindDefinite, Err: err}
	}
	switch de.Kind {
	case KindAmbiguous:
		s.recordf(s.st.MarkAmbiguous(ctx, intent.ID), "mark ambiguous", intent.ID)
		s.reconcile(ctx, d, intent)
	case KindPermanent:
		s.recordf(s.st.MarkFailed(ctx, intent.ID, "permanent platform failure"), "mark failed", intent.ID)
	case KindRateLimited:
		wait := de.RetryAfter
		if wait <= 0 {
			wait = backoff(intent.Attempts)
		}
		s.recordf(s.st.MarkAttempt(ctx, intent.ID, time.Now().Add(min(wait, 5*time.Minute))), "mark attempt", intent.ID)
	default:
		s.recordf(s.st.MarkAttempt(ctx, intent.ID, time.Now().Add(backoff(intent.Attempts))), "mark attempt", intent.ID)
	}
	return false, true
}

// reconcile tries once to identify an own message for an ambiguous create.
// Association only establishes the remote ID; the desired revision stays
// pending until an acknowledged edit.
func (s *Service) reconcile(ctx context.Context, d Deliverer, row state.Delivery) {
	rctx, cancel := context.WithTimeout(ctx, s.platformTimeout())
	id, found, err := d.Reconcile(rctx, row.Destination, row.Nonce)
	cancel()
	if err == nil && found {
		s.recordf(s.st.AssociateMessage(ctx, row.ID, id, "reconciled by nonce"), "associate reconciled message", row.ID)
		return
	}
	s.log.Warn("delivery create ambiguous; operator resolution required", "delivery", row.ID)
}

// editDelivery applies the latest desired revision to a known remote message.
func (s *Service) editDelivery(ctx context.Context, d Deliverer, row state.Delivery) (ok, progressed bool) {
	if exhausted(row) {
		s.recordf(s.st.MarkFailed(ctx, row.ID, "automated attempts exhausted"), "mark failed", row.ID)
		return false, true
	}
	if !row.HasDesiredText {
		return s.st.MarkEdited(ctx, row.ID, row.DesiredRevision) == nil, true
	}
	if err := s.throttle(ctx, row.Destination); err != nil {
		return false, false
	}
	ectx, cancel := context.WithTimeout(ctx, s.platformTimeout())
	err := d.Edit(ectx, row.Destination, row.RemoteID, row.DesiredText)
	cancel()
	if err == nil {
		if err := s.st.MarkEdited(ctx, row.ID, row.DesiredRevision); err != nil {
			s.recordf(err, "mark edited", row.ID)
			return false, false
		}
		return true, true
	}
	var de *DeliveryError
	if !errors.As(err, &de) {
		de = &DeliveryError{Kind: KindDefinite, Err: err}
	}
	switch de.Kind {
	case KindPermanent:
		s.recordf(s.st.MarkFailed(ctx, row.ID, "permanent platform failure"), "mark failed", row.ID)
	case KindRateLimited:
		wait := de.RetryAfter
		if wait <= 0 {
			wait = backoff(row.Attempts + 1)
		}
		s.recordf(s.st.MarkAttempt(ctx, row.ID, time.Now().Add(min(wait, 5*time.Minute))), "mark attempt", row.ID)
	default:
		s.recordf(s.st.MarkAttempt(ctx, row.ID, time.Now().Add(backoff(row.Attempts+1))), "mark attempt", row.ID)
	}
	return false, true
}

// drainDeliveries advances the ordered lane of a route. It returns whether
// the lane is blocked for operator resolution and how long to wait before
// the next automated retry (0 when idle). A pass that changes no durable
// state parks the route instead of spinning.
func (s *Service) drainDeliveries(conv *state.Conversation) (blocked bool, wait time.Duration) {
	ctx := s.ctx
	key := conv.Route.Key()
	for {
		if ctx.Err() != nil {
			return false, 0
		}
		row, isBlocked, err := s.st.NextDelivery(ctx, key)
		if isNotFound(err) {
			return false, 0
		}
		if err != nil {
			return false, parkUnavailable
		}
		if isBlocked {
			return true, 0
		}
		d := s.deliverers[row.Destination.Platform]
		if d == nil {
			s.recordf(s.st.MarkFailed(ctx, row.ID, "no adapter for platform"), "mark failed", row.ID)
			return true, 0
		}
		actx, acancel := context.WithTimeout(ctx, s.platformTimeout())
		allowed, aerr := d.Allowed(actx, row.Destination)
		acancel()
		if aerr != nil {
			s.log.Warn("destination check unavailable; retrying later", "delivery", row.ID, "error", safeErr(aerr))
			return false, parkUnavailable
		}
		if !allowed {
			s.recordf(s.st.MarkFailed(ctx, row.ID, "destination no longer allowed"), "mark failed", row.ID)
			return true, 0
		}
		current, err := s.st.GetConversationByKey(ctx, key)
		if err != nil {
			return false, parkUnavailable
		}
		*conv = current
		if row.Generation != current.Generation {
			s.recordf(s.st.MarkFailed(ctx, row.ID, "conversation generation rotated"), "mark failed", row.ID)
			return true, 0
		}
		if w := time.Until(row.RetryAt); w > 0 {
			return false, w
		}
		var progressed bool
		if row.RemoteID == "" {
			_, progressed = s.createDelivery(ctx, d, row)
		} else {
			_, progressed = s.editDelivery(ctx, d, row)
		}
		if !progressed {
			return false, parkUnavailable
		}
	}
}
