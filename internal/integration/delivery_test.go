package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/state"
	"github.com/mattsp1290/eino-channels/internal/testkit"
)

// Rate-limited creates wait for Retry-After and then succeed; permanent
// failures mark the row failed without new inference.
func TestDeliveryRateLimitThenPermanentFailure(t *testing.T) {
	dir := t.TempDir()
	provider := newFakeOpenCode(t)
	env := openReal(t, dir, provider, config.DefaultLimits())
	defer env.close()
	var attempts int
	env.slack.FailCreate = func(_ state.Destination, _ string) error {
		attempts++
		if attempts == 1 {
			return &conversation.DeliveryError{Kind: conversation.KindRateLimited, RetryAfter: time.Second, Err: errors.New("429")}
		}
		return nil
	}
	r := env.ingest(testkit.DM("D1", "U1", "6.000001", "rl"))
	texts := env.waitDelivered(env.slack, r.Item.ID)
	if texts[0] != "reply:rl" || attempts < 2 {
		t.Fatalf("texts=%q attempts=%d", texts, attempts)
	}
	env.slack.FailCreate = func(_ state.Destination, _ string) error {
		return &conversation.DeliveryError{Kind: conversation.KindPermanent, Err: errors.New("channel_not_found")}
	}
	r2 := env.ingest(testkit.DM("D2", "U2", "6.000002", "perm"))
	testkit.Eventually(t, 30*time.Second, func() bool {
		it, err := env.store.GetItem(context.Background(), r2.Item.ID)
		if err != nil || it.State != state.StateTerminal {
			return false
		}
		row, blocked, err := env.store.NextDelivery(context.Background(), it.RouteKey)
		return err == nil && blocked && row.Status == state.DeliveryFailed
	}, "permanent failure recorded")
	if n := len(provider.requests()); n != 2 {
		t.Fatalf("requests=%d", n)
	}
	rows, _ := env.store.ListDeliveries(context.Background(), 10)
	if len(rows) != 1 || rows[0].Status != state.DeliveryFailed {
		t.Fatalf("rows=%+v", rows)
	}
	// Removed allowlist access prevents sending stored output.
	env.slack.FailCreate = nil
	env.slack.Deny = func(d state.Destination) bool { return d.DMActor == "U3" }
	r3 := env.ingest(testkit.DM("D3", "U3", "6.000003", "denied"))
	testkit.Eventually(t, 30*time.Second, func() bool {
		it, err := env.store.GetItem(context.Background(), r3.Item.ID)
		if err != nil || it.State != state.StateTerminal {
			return false
		}
		row, blocked, err := env.store.NextDelivery(context.Background(), it.RouteKey)
		return err == nil && blocked && row.Status == state.DeliveryFailed && row.Audit == "destination no longer allowed"
	}, "denied destination")
}
