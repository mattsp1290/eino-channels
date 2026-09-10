package conversation_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/state"
	"github.com/mattsp1290/eino-channels/internal/testkit"
)

// A pending delivery must survive a startup window in which the adapter
// cannot yet verify destinations (Discord before Ready): the lane parks
// instead of failing the row.
func TestPendingDeliverySurvivesAdapterNotReady(t *testing.T) {
	dir := t.TempDir()
	script := testkit.NewScript()
	d := testkit.NewDeliverer(1800)
	// Process A: produce a terminal run whose delivery never completes.
	d.FailCreate = func(state.Destination, string) error {
		return &conversation.DeliveryError{Kind: conversation.KindDefinite, Err: errors.New("down")}
	}
	env := testkit.Open(t, testkit.Options{Dir: dir, Script: script, Deliverer: d, Started: true, Platform: state.PlatformDiscord})
	r := env.Ingest(state.Inbound{Route: state.Route{Platform: state.PlatformDiscord, Installation: "BOT", Channel: "700000000000000001", DMActor: "300000000000000001"}, MessageID: "800000000000000001", Actor: "300000000000000001", ActorLabel: "x", Content: "hello", ReceivedAt: time.Now()})
	testkit.Eventually(t, wait, func() bool {
		it, _ := env.Store.GetItem(context.Background(), r.Item.ID)
		return it.State == state.StateTerminal
	}, "terminal")
	env.Close()

	// Process B: the adapter has no identity yet.
	d.FailCreate = nil
	d.SetAllowedErr(errors.New("identity not established yet"))
	env = testkit.Open(t, testkit.Options{Dir: dir, Script: script, Deliverer: d, Started: true, Platform: state.PlatformDiscord})
	defer env.Close()
	time.Sleep(3 * time.Second) // several scheduler passes
	row, blocked, err := env.Store.NextDelivery(context.Background(), r.Item.RouteKey)
	if err != nil || blocked || row.Status != state.DeliveryPending {
		t.Fatalf("row=%+v blocked=%v err=%v", row, blocked, err)
	}
	for _, c := range d.Calls() {
		if c.Op == "create" {
			t.Fatal("created while identity unknown")
		}
	}
	// Identity arrives (and a new ingest wakes the parked route).
	d.SetAllowedErr(nil)
	route := state.Route{Platform: state.PlatformDiscord, Installation: "BOT", Channel: "700000000000000001", DMActor: "300000000000000001"}
	r2 := env.Ingest(state.Inbound{Route: route, MessageID: "800000000000000002", Actor: "300000000000000001", ActorLabel: "x", Content: "again", ReceivedAt: time.Now()})
	if texts := env.WaitDelivered(r.Item.ID, wait); texts[0] != "reply:hello" {
		t.Fatalf("delivered=%q", texts)
	}
	env.WaitDelivered(r2.Item.ID, wait)
}

// A create intent without a recorded outcome is the central crash window:
// after a restart it becomes ambiguous and is never created again blindly.
func TestLeftoverCreateIntentBecomesAmbiguousOnRestart(t *testing.T) {
	dir := t.TempDir()
	script := testkit.NewScript()
	d := testkit.NewDeliverer(3500)
	env := testkit.Open(t, testkit.Options{Dir: dir, Script: script, Deliverer: d})
	r := env.Ingest(testkit.DM("D1", "U1", "13.000001", "x"))
	if err := env.Store.Transition(context.Background(), r.Item.ID, state.StateQueued, state.StateAdmitting, ""); err != nil {
		t.Fatal(err)
	}
	if err := env.Store.MarkAdmitted(context.Background(), r.Item.ID, "run-13", "u", "a"); err != nil {
		t.Fatal(err)
	}
	if err := env.Store.MarkTerminal(context.Background(), r.Item.ID, "completed", state.CodeCompleted, []state.DeliveryPlan{{ChunkIndex: 0, Text: "answer", Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	row, err := env.Store.DeliveryForRun(context.Background(), "run-13", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.Store.MarkCreateIntent(context.Background(), row.ID); err != nil {
		t.Fatal(err) // crash happens here: intent recorded, outcome unknown
	}
	env.Close()

	env = testkit.Open(t, testkit.Options{Dir: dir, Script: script, Deliverer: d, Started: true})
	defer env.Close()
	testkit.Eventually(t, wait, func() bool {
		got, err := env.Store.GetDelivery(context.Background(), row.ID)
		return err == nil && got.Status == state.DeliveryAmbiguous
	}, "ambiguous after restart")
	for _, c := range d.Calls() {
		if c.Op == "create" {
			t.Fatalf("blind re-create: %+v", c)
		}
	}
	rec := 0
	for _, c := range d.Calls() {
		if c.Op == "reconcile" {
			rec++
		}
	}
	if rec != 1 {
		t.Fatalf("reconcile attempts=%d", rec)
	}
}

// Notices render the configured prompt limit, not a hard-coded one.
func TestNoticesRenderConfiguredPromptLimit(t *testing.T) {
	limits := testkitLimits()
	limits.MaxPromptBytes = 4096
	env := testkit.Open(t, testkit.Options{Started: true, Limits: limits})
	defer env.Close()
	help := env.Ingest(testkit.DM("D1", "U1", "14.000001", "!help"))
	if !strings.Contains(help.Notice, "prompts up to 4 KiB") {
		t.Fatalf("help=%q", help.Notice)
	}
	big := env.Ingest(testkit.DM("D1", "U1", "14.000002", strings.Repeat("y", 5000)))
	if big.Outcome != state.OutcomeRejected || !strings.Contains(big.Notice, "under 4 KiB") {
		t.Fatalf("oversize=%+v", big)
	}
	if conversation.NoticeHelp != "" && !strings.Contains(conversation.NoticeHelp, "16 KiB") {
		t.Fatalf("default help=%q", conversation.NoticeHelp)
	}
}
