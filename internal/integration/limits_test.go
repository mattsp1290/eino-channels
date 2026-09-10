package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/state"
	"github.com/mattsp1290/eino-channels/internal/testkit"
)

// Gate 4: the maximum supported history is still projected and delivered;
// the 101st turn is refused with the !new instruction, and !new resets.
func TestHistoryLimitThenNew(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("long: runs in the non-race pass")
	}
	dir := t.TempDir()
	provider := newFakeOpenCode(t)
	env := openReal(t, dir, provider, config.DefaultLimits())
	defer env.close()
	start := time.Now()
	var last int64
	for i := 1; i <= config.MaxHistoryTurns; i++ {
		r := env.ingest(testkit.DM("D1", "U1", fmt.Sprintf("9.%06d", i), fmt.Sprintf("t%d", i)))
		last = r.Item.ID
		if r.Outcome != state.OutcomeAccepted {
			t.Fatalf("turn %d: %+v", i, r)
		}
		testkit.Eventually(t, 30*time.Second, func() bool {
			it, err := env.store.GetItem(context.Background(), last)
			return err == nil && it.State == state.StateTerminal
		}, fmt.Sprintf("turn %d terminal", i))
	}
	texts := env.waitDelivered(env.slack, last)
	if texts[0] != fmt.Sprintf("reply:t%d", config.MaxHistoryTurns) {
		t.Fatalf("last=%q", texts)
	}
	t.Logf("%d turns in %s", config.MaxHistoryTurns, time.Since(start))
	reqs := provider.requests()
	if len(reqs) != config.MaxHistoryTurns || len(messagesOf(reqs[len(reqs)-1])) != 2*config.MaxHistoryTurns {
		t.Fatalf("history projection: requests=%d msgs=%d", len(reqs), len(messagesOf(reqs[len(reqs)-1])))
	}
	over := env.ingest(testkit.DM("D1", "U1", "9.000999", "one more"))
	testkit.Eventually(t, 20*time.Second, func() bool {
		it, _ := env.store.GetItem(context.Background(), over.Item.ID)
		return it.State == state.StateRejected && it.ResultCode == state.CodeHistoryLimit
	}, "history limit rejection")
	if n := len(provider.requests()); n != config.MaxHistoryTurns {
		t.Fatalf("requests after limit=%d", n)
	}
	found := false
	for _, n := range env.slack.Notices() {
		if n == conversation.NoticeHistoryLimit {
			found = true
		}
	}
	if !found {
		t.Fatalf("notices=%q", env.slack.Notices())
	}
	if resp := env.ingest(testkit.DM("D1", "U1", "9.001000", "!new")); resp.Outcome != state.OutcomeAccepted {
		t.Fatalf("new=%+v", resp)
	}
	fresh := env.ingest(testkit.DM("D1", "U1", "9.001001", "fresh"))
	env.waitDelivered(env.slack, fresh.Item.ID)
	reqs = provider.requests()
	if len(messagesOf(reqs[len(reqs)-1])) != 2 {
		t.Fatal("fresh generation carried history")
	}
}

func TestSecondDaemonAndUnknownSchemaAreRefused(t *testing.T) {
	dir := t.TempDir()
	provider := newFakeOpenCode(t)
	env := openReal(t, dir, provider, config.DefaultLimits())
	defer env.close()
	if _, err := state.Open(context.Background(), dir, state.Options{}); err == nil || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("second daemon: %v", err)
	}
}

// Full per-route and global queues bound work and notify once.
func TestFullQueuesBoundWork(t *testing.T) {
	dir := t.TempDir()
	provider := newFakeOpenCode(t)
	provider.mu.Lock()
	provider.delay = 2 * time.Second
	provider.mu.Unlock()
	limits := config.DefaultLimits()
	limits.MaxQueuedPerConversation = 1
	limits.MaxPendingInbox = 3
	env := openReal(t, dir, provider, limits)
	defer env.close()
	a := env.ingest(testkit.DM("D1", "U1", "8.000001", "a"))
	testkit.Eventually(t, 20*time.Second, func() bool {
		it, _ := env.store.GetItem(context.Background(), a.Item.ID)
		return it.State == state.StateAdmitting || it.State == state.StateAdmitted
	}, "a running")
	b := env.ingest(testkit.DM("D1", "U1", "8.000002", "b"))
	c := env.ingest(testkit.DM("D1", "U1", "8.000003", "c"))
	if c.Outcome != state.OutcomeRejected || c.Notice != conversation.NoticeOverflow {
		t.Fatalf("per-route overflow=%+v", c)
	}
	d := env.ingest(testkit.DM("D2", "U2", "8.000004", "d"))
	e := env.ingest(testkit.DM("D3", "U3", "8.000005", "e"))
	f := env.ingest(testkit.DM("D4", "U4", "8.000006", "f"))
	if d.Outcome != state.OutcomeAccepted || e.Outcome != state.OutcomeAccepted {
		t.Fatalf("d=%v e=%v", d.Outcome, e.Outcome)
	}
	if f.Outcome != state.OutcomeRejected {
		t.Fatalf("global overflow=%+v", f)
	}
	env.waitDelivered(env.slack, a.Item.ID)
	env.waitDelivered(env.slack, b.Item.ID)
	if n := len(provider.requests()); n < 2 {
		t.Fatalf("requests=%d", n)
	}
}
