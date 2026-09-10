package conversation

import (
	"context"
	"errors"
	"time"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/redact"
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
			// Give the slot back: nothing was dispatched.
			s.limiterMu.Lock()
			s.limiterLast[dest] = last
			s.limiterMu.Unlock()
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
		s.log.Error(msg, "delivery", id, "error", redact.Err(err))
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
		select {
		case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
		case <-s.ctx.Done():
			// One last immediate attempt so a create that landed is recorded.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = s.st.MarkCreated(ctx, id, remoteID, revision)
			cancel()
			return err
		}
	}
	return err
}

// createOutcome classifies one createDelivery pass.
type createOutcome int

const (
	createBlocked   createOutcome = iota // nothing durable changed; park the lane
	createFailed                         // a failure or ambiguity was recorded
	createSucceeded                      // the message exists and is recorded
)

// retryMark records a retry schedule for a row.
type retryMark func(ctx context.Context, id int64, retryAt time.Time) error

// recordFailure classifies a platform error and records the matching
// durable state: permanent → failed, rate limited → wait for the server's
// hint, otherwise → backoff. ambiguous, when non-nil, handles the
// create-only unknown-outcome case.
func (s *Service) recordFailure(ctx context.Context, row state.Delivery, err error, attempts int, mark retryMark, ambiguous func()) {
	var de *DeliveryError
	if !errors.As(err, &de) {
		de = &DeliveryError{Kind: KindDefinite, Err: err}
	}
	switch de.Kind {
	case KindAmbiguous:
		if ambiguous != nil {
			ambiguous()
			return
		}
		s.recordf(mark(ctx, row.ID, time.Now().Add(backoff(attempts))), "mark retry", row.ID)
	case KindPermanent:
		s.recordf(s.st.MarkFailed(ctx, row.ID, "permanent platform failure"), "mark failed", row.ID)
	case KindRateLimited:
		wait := de.RetryAfter
		if wait <= 0 {
			wait = backoff(attempts)
		}
		s.recordf(mark(ctx, row.ID, time.Now().Add(min(wait, 5*time.Minute))), "mark retry", row.ID)
	default:
		s.recordf(mark(ctx, row.ID, time.Now().Add(backoff(attempts))), "mark retry", row.ID)
	}
}

// createDelivery persists an intent, performs one create, and records the
// outcome.
func (s *Service) createDelivery(ctx context.Context, d Deliverer, row state.Delivery) createOutcome {
	if exhausted(row) {
		s.recordf(s.st.MarkFailed(ctx, row.ID, "automated attempts exhausted"), "mark failed", row.ID)
		return createFailed
	}
	if row.Op == state.OpCreateIntent && row.Status == state.DeliveryPending {
		// An intent without a recorded outcome: a previous process may have
		// created the message. Never create again blindly.
		s.recordf(s.st.MarkAmbiguous(ctx, row.ID), "mark ambiguous", row.ID)
		s.reconcile(ctx, d, row)
		return createFailed
	}
	if !row.HasDesiredText {
		s.recordf(s.st.MarkEdited(ctx, row.ID, row.DesiredRevision), "acknowledge empty payload", row.ID)
		return createFailed
	}
	// Wait for the rate-limit slot before recording the intent: an intent
	// with no recorded outcome is treated as a possible send.
	if err := s.throttle(ctx, row.Destination); err != nil {
		return createBlocked
	}
	intent, err := s.st.MarkCreateIntent(ctx, row.ID)
	if err != nil {
		s.recordf(err, "mark create intent", row.ID)
		return createBlocked
	}
	cctx, cancel := context.WithTimeout(ctx, s.platformTimeout())
	remoteID, err := d.Create(cctx, intent.Destination, intent.DesiredText, intent.Nonce)
	cancel()
	if err == nil {
		if err := s.persistCreated(intent.ID, remoteID, intent.DesiredRevision); err != nil {
			s.log.Error("create succeeded but could not be recorded; row left as intent for ambiguity handling", "delivery", intent.ID, "error", redact.Err(err))
			return createBlocked
		}
		return createSucceeded
	}
	// The intent already counted this attempt, so schedule without counting again.
	s.recordFailure(ctx, intent, err, intent.Attempts, s.st.MarkRetryAt, func() {
		s.recordf(s.st.MarkAmbiguous(ctx, intent.ID), "mark ambiguous", intent.ID)
		s.reconcile(ctx, d, intent)
	})
	return createFailed
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

// editDelivery applies the latest desired revision to a known remote
// message. It reports whether durable state changed.
func (s *Service) editDelivery(ctx context.Context, d Deliverer, row state.Delivery) (progressed bool) {
	if exhausted(row) {
		s.recordf(s.st.MarkFailed(ctx, row.ID, "automated attempts exhausted"), "mark failed", row.ID)
		return true
	}
	if !row.HasDesiredText {
		s.recordf(s.st.MarkEdited(ctx, row.ID, row.DesiredRevision), "acknowledge empty payload", row.ID)
		return true
	}
	if err := s.throttle(ctx, row.Destination); err != nil {
		return false
	}
	ectx, cancel := context.WithTimeout(ctx, s.platformTimeout())
	err := d.Edit(ectx, row.Destination, row.RemoteID, row.DesiredText)
	cancel()
	if err == nil {
		if err := s.st.MarkEdited(ctx, row.ID, row.DesiredRevision); err != nil {
			s.recordf(err, "mark edited", row.ID)
			return false
		}
		return true
	}
	s.recordFailure(ctx, row, err, row.Attempts+1, s.st.MarkAttempt, nil)
	return true
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
		current, err := s.st.GetConversationByKey(ctx, key)
		if err != nil {
			return false, parkUnavailable
		}
		*conv = current
		actx, acancel := context.WithTimeout(ctx, s.platformTimeout())
		allowed, aerr := d.Allowed(actx, row.Destination, current.Route.Subject())
		acancel()
		if aerr != nil {
			s.log.Warn("destination check unavailable; retrying later", "delivery", row.ID, "error", redact.Err(aerr))
			return false, parkUnavailable
		}
		if !allowed {
			s.recordf(s.st.MarkFailed(ctx, row.ID, "destination no longer allowed"), "mark failed", row.ID)
			return true, 0
		}
		if row.Generation != current.Generation {
			s.recordf(s.st.MarkFailed(ctx, row.ID, "conversation generation rotated"), "mark failed", row.ID)
			return true, 0
		}
		if w := time.Until(row.RetryAt); w > 0 {
			return false, w
		}
		progressed := true
		if row.RemoteID == "" {
			progressed = s.createDelivery(ctx, d, row) != createBlocked
		} else {
			progressed = s.editDelivery(ctx, d, row)
		}
		if !progressed {
			return false, parkUnavailable
		}
	}
}
