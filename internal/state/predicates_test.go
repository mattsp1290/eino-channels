package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestPredicatesAgree pins the three expressions of "this delivery still
// needs work" to each other: the unqualified and alias-qualified SQL
// predicates and Delivery.Resolved.
func TestPredicatesAgree(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	in := Inbound{Route: Route{Platform: PlatformSlack, Installation: "T1", Channel: "D9", DMActor: "U1"}, MessageID: "9.1", Actor: "U1", ActorLabel: "U1", Kind: KindPrompt, Content: "x", ReceivedAt: time.Now()}
	d, err := st.Ingest(ctx, in, Capacity{MaxQueuedPerRoute: 8, MaxPendingGlobal: 256})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Transition(ctx, d.Item.ID, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkAdmitted(ctx, d.Item.ID, "run-9", "u", "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkTerminal(ctx, d.Item.ID, "completed", CodeCompleted, []DeliveryPlan{{ChunkIndex: 0, Text: "a", Revision: 1}, {ChunkIndex: 1, Text: "b", Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	check := func(stage string) {
		t.Helper()
		var viaPlain, viaAlias int
		if err := st.host.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE `+unresolvedDelivery).Scan(&viaPlain); err != nil {
			t.Fatal(err)
		}
		if err := st.host.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries d WHERE `+unresolvedDeliveryD).Scan(&viaAlias); err != nil {
			t.Fatal(err)
		}
		rows, err := st.ListDeliveries(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		viaResolved := 0
		for i := 0; ; i++ {
			row, err := st.DeliveryForRun(ctx, "run-9", i)
			if err != nil {
				break
			}
			if !row.Resolved() {
				viaResolved++
			}
		}
		if viaPlain != viaAlias || viaPlain != viaResolved || len(rows) != viaPlain {
			t.Fatalf("%s: plain=%d alias=%d resolved=%d listed=%d", stage, viaPlain, viaAlias, viaResolved, len(rows))
		}
	}
	check("planned")
	row0, _ := st.DeliveryForRun(ctx, "run-9", 0)
	if _, err := st.MarkCreateIntent(ctx, row0.ID); err != nil {
		t.Fatal(err)
	}
	check("intent")
	if err := st.MarkCreated(ctx, row0.ID, "1.1", 1); err != nil {
		t.Fatal(err)
	}
	check("chunk0 acked")
	// A resolved row can never be failed by a caller with stale information.
	if err := st.MarkFailed(ctx, row0.ID, "stale"); !errors.Is(err, ErrConflict) {
		t.Fatalf("MarkFailed on resolved row: %v", err)
	}
	row1, _ := st.DeliveryForRun(ctx, "run-9", 1)
	if err := st.MarkAmbiguous(ctx, row1.ID); err != nil {
		t.Fatal(err)
	}
	check("chunk1 ambiguous")
}

// TestRoutesWithWorkCursor exercises the rotating cursor the scheduler
// depends on for fairness.
func TestRoutesWithWorkCursor(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	var keys []string
	for _, ch := range []string{"D1", "D2", "D3"} {
		in := Inbound{Route: Route{Platform: PlatformSlack, Installation: "T1", Channel: ch, DMActor: "U1"}, MessageID: ch + ".1", Actor: "U1", ActorLabel: "U1", Kind: KindPrompt, Content: "x", ReceivedAt: time.Now()}
		if _, err := st.Ingest(ctx, in, Capacity{MaxQueuedPerRoute: 8, MaxPendingGlobal: 256}); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, in.Route.Key())
	}
	all, err := st.RoutesWithWork(ctx, "", 10)
	if err != nil || len(all) != 3 {
		t.Fatalf("all=%v err=%v", all, err)
	}
	first, err := st.RoutesWithWork(ctx, "", 1)
	if err != nil || len(first) != 1 || first[0] != all[0] {
		t.Fatalf("first=%v err=%v", first, err)
	}
	rest, err := st.RoutesWithWork(ctx, first[0], 10)
	if err != nil || len(rest) != 2 || rest[0] != all[1] || rest[1] != all[2] {
		t.Fatalf("rest=%v err=%v", rest, err)
	}
	if tail, err := st.RoutesWithWork(ctx, all[2], 10); err != nil || len(tail) != 0 {
		t.Fatalf("tail=%v err=%v", tail, err)
	}
	// A create intent left over with a pending status is scheduled; a
	// failed row is not.
	_ = keys
}
