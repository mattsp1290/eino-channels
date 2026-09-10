package conversation

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/state"
)

var (
	limiterMu   sync.Mutex
	limiterLast = map[state.Destination]time.Time{}
)

// throttle enforces the minimum spacing between calls per destination.
func throttle(ctx context.Context, dest state.Destination, spacing time.Duration) {
	limiterMu.Lock()
	last := limiterLast[dest]
	wait := time.Until(last.Add(spacing))
	if wait > 0 {
		limiterLast[dest] = last.Add(spacing)
	} else {
		limiterLast[dest] = time.Now()
	}
	limiterMu.Unlock()
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
		}
	}
}

func backoff(attempts int) time.Duration {
	d := 2 * time.Second
	for i := 1; i < attempts && d < 2*time.Minute; i++ {
		d *= 2
	}
	return d
}

// createDelivery persists an intent, performs one create, and records the
// outcome. ok is false when the row cannot proceed now.
func (s *Service) createDelivery(ctx context.Context, d Deliverer, row state.Delivery) (state.Delivery, bool) {
	if row.Attempts >= config.DeliveryMaxAttempts || !row.FirstAttemptAt.IsZero() && time.Since(row.FirstAttemptAt) > config.DeliveryWindowMinutes*time.Minute {
		_ = s.st.MarkFailed(ctx, row.ID, "automated attempts exhausted")
		return row, false
	}
	intent, err := s.st.MarkCreateIntent(ctx, row.ID)
	if err != nil {
		return row, false
	}
	if !intent.HasDesiredText {
		// Nothing left to send: acknowledge at the desired revision.
		_ = s.st.MarkEdited(ctx, intent.ID, intent.DesiredRevision)
		return row, false
	}
	throttle(ctx, intent.Destination, s.spacing)
	cctx, cancel := context.WithTimeout(ctx, s.platformTimeout())
	remoteID, err := d.Create(cctx, intent.Destination, intent.DesiredText, intent.Nonce)
	cancel()
	if err == nil {
		if err := s.st.MarkCreated(ctx, intent.ID, remoteID, intent.DesiredRevision); err != nil {
			return row, false
		}
		out, err := s.st.GetDelivery(ctx, intent.ID)
		return out, err == nil
	}
	var de *DeliveryError
	if !errors.As(err, &de) {
		de = &DeliveryError{Kind: KindDefinite, Err: err}
	}
	switch de.Kind {
	case KindAmbiguous:
		_ = s.st.MarkAmbiguous(ctx, intent.ID)
		rctx, rcancel := context.WithTimeout(ctx, s.platformTimeout())
		id, found, rerr := d.Reconcile(rctx, intent.Destination, intent.Nonce)
		rcancel()
		if rerr == nil && found {
			// Association only establishes the remote ID; the desired revision stays pending.
			if err := s.st.AssociateMessage(ctx, intent.ID, id, "reconciled by nonce"); err == nil {
				out, err := s.st.GetDelivery(ctx, intent.ID)
				return out, err == nil
			}
		}
		s.log.Warn("delivery create ambiguous; operator resolution required", "delivery", intent.ID)
	case KindPermanent:
		_ = s.st.MarkFailed(ctx, intent.ID, "permanent platform failure")
	case KindRateLimited:
		wait := de.RetryAfter
		if wait <= 0 {
			wait = backoff(intent.Attempts)
		}
		_ = s.st.MarkAttempt(ctx, intent.ID, time.Now().Add(min(wait, 5*time.Minute)))
	default:
		_ = s.st.MarkAttempt(ctx, intent.ID, time.Now().Add(backoff(intent.Attempts)))
	}
	return row, false
}

// editDelivery applies the latest desired revision to a known remote message.
func (s *Service) editDelivery(ctx context.Context, d Deliverer, row state.Delivery) bool {
	if row.Attempts >= config.DeliveryMaxAttempts || !row.FirstAttemptAt.IsZero() && time.Since(row.FirstAttemptAt) > config.DeliveryWindowMinutes*time.Minute {
		_ = s.st.MarkFailed(ctx, row.ID, "automated attempts exhausted")
		return false
	}
	if !row.HasDesiredText {
		_ = s.st.MarkEdited(ctx, row.ID, row.DesiredRevision)
		return true
	}
	throttle(ctx, row.Destination, s.spacing)
	ectx, cancel := context.WithTimeout(ctx, s.platformTimeout())
	err := d.Edit(ectx, row.Destination, row.RemoteID, row.DesiredText)
	cancel()
	if err == nil {
		return s.st.MarkEdited(ctx, row.ID, row.DesiredRevision) == nil
	}
	var de *DeliveryError
	if !errors.As(err, &de) {
		de = &DeliveryError{Kind: KindDefinite, Err: err}
	}
	switch de.Kind {
	case KindPermanent:
		_ = s.st.MarkFailed(ctx, row.ID, "permanent platform failure")
	case KindRateLimited:
		wait := de.RetryAfter
		if wait <= 0 {
			wait = backoff(row.Attempts + 1)
		}
		_ = s.st.MarkAttempt(ctx, row.ID, time.Now().Add(min(wait, 5*time.Minute)))
	default:
		_ = s.st.MarkAttempt(ctx, row.ID, time.Now().Add(backoff(row.Attempts+1)))
	}
	return false
}

// drainDeliveries advances the ordered lane of a route. It returns whether
// the lane is blocked for operator resolution and how long to wait before
// the next automated retry (0 when idle).
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
			return false, parkUnavailble
		}
		if isBlocked {
			return true, 0
		}
		d := s.deliverers[row.Destination.Platform]
		if d == nil || !d.Allowed(row.Destination) {
			_ = s.st.MarkFailed(ctx, row.ID, "destination no longer allowed")
			return true, 0
		}
		current, err := s.st.GetConversationByKey(ctx, key)
		if err != nil {
			return false, parkUnavailble
		}
		*conv = current
		if row.Generation != current.Generation {
			_ = s.st.MarkFailed(ctx, row.ID, "conversation generation rotated")
			return true, 0
		}
		if w := time.Until(row.RetryAt); w > 0 {
			return false, w
		}
		if row.RemoteID == "" {
			if _, ok := s.createDelivery(ctx, d, row); !ok {
				continue
			}
			continue
		}
		if !s.editDelivery(ctx, d, row) {
			continue
		}
	}
}
