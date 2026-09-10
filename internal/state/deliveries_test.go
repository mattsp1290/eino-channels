package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 9. Full delivery lifecycle.
func TestDeliveryLifecycle(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()
	route := slackRoute()

	d, err := st.Ingest(ctx, promptInbound(route, "dl1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	id := d.Item.ID
	if err := st.Transition(ctx, id, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if err := st.MarkAdmitted(ctx, id, "run-dl", "m1", "m2"); err != nil {
		t.Fatalf("MarkAdmitted: %v", err)
	}
	plans := []DeliveryPlan{{ChunkIndex: 0, Text: "chunk-a", Revision: 1}, {ChunkIndex: 1, Text: "chunk-b", Revision: 1}}
	if err := st.MarkTerminal(ctx, id, "completed", CodeCompleted, plans); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	routeKey := route.Key()

	n, err := st.UnresolvedDeliveries(ctx, routeKey)
	if err != nil {
		t.Fatalf("UnresolvedDeliveries: %v", err)
	}
	if n != 2 {
		t.Fatalf("unresolved = %d, want 2", n)
	}

	nd, blocked, err := st.NextDelivery(ctx, routeKey)
	if err != nil {
		t.Fatalf("NextDelivery: %v", err)
	}
	if nd.ChunkIndex != 0 {
		t.Fatalf("chunk = %d, want 0", nd.ChunkIndex)
	}
	if nd.Status != DeliveryPending {
		t.Fatalf("status = %v, want pending", nd.Status)
	}
	if blocked {
		t.Fatalf("blocked = true, want false")
	}
	d0ID := nd.ID

	mi, err := st.MarkCreateIntent(ctx, d0ID)
	if err != nil {
		t.Fatalf("MarkCreateIntent: %v", err)
	}
	if mi.Op != OpCreateIntent {
		t.Fatalf("op = %v, want create_intent", mi.Op)
	}
	if mi.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", mi.Attempts)
	}
	if len(mi.Nonce) != 24 {
		t.Fatalf("nonce len = %d, want 24 (nonce = %q)", len(mi.Nonce), mi.Nonce)
	}
	if mi.FirstAttemptAt.IsZero() {
		t.Fatalf("first attempt at is zero")
	}

	if err := st.MarkCreated(ctx, d0ID, "9.9", 1); err != nil {
		t.Fatalf("MarkCreated: %v", err)
	}
	d0, err := st.GetDelivery(ctx, d0ID)
	if err != nil {
		t.Fatalf("GetDelivery d0: %v", err)
	}
	if d0.Status != DeliveryAcked {
		t.Fatalf("status = %v, want acked", d0.Status)
	}
	if d0.AckedRevision != 1 {
		t.Fatalf("acked revision = %d, want 1", d0.AckedRevision)
	}
	if d0.RemoteID != "9.9" {
		t.Fatalf("remote id = %q, want 9.9", d0.RemoteID)
	}
	if d0.HasDesiredText {
		t.Fatalf("has desired text = true, want false")
	}
	if !d0.Resolved() {
		t.Fatalf("d0 not resolved")
	}

	nd2, blocked2, err := st.NextDelivery(ctx, routeKey)
	if err != nil {
		t.Fatalf("NextDelivery 2: %v", err)
	}
	if nd2.ChunkIndex != 1 {
		t.Fatalf("chunk = %d, want 1", nd2.ChunkIndex)
	}
	if blocked2 {
		t.Fatalf("blocked2 = true, want false")
	}
	d1ID := nd2.ID

	if err := st.MarkAmbiguous(ctx, d1ID); err != nil {
		t.Fatalf("MarkAmbiguous: %v", err)
	}
	nd3, blocked3, err := st.NextDelivery(ctx, routeKey)
	if err != nil {
		t.Fatalf("NextDelivery 3: %v", err)
	}
	if nd3.ID != d1ID {
		t.Fatalf("next delivery id = %d, want %d", nd3.ID, d1ID)
	}
	if nd3.Status != DeliveryAmbiguous {
		t.Fatalf("status = %v, want ambiguous_create", nd3.Status)
	}
	if !blocked3 {
		t.Fatalf("blocked3 = false, want true")
	}

	if err := st.AssociateMessage(ctx, d1ID, "7.7", "operator"); err != nil {
		t.Fatalf("AssociateMessage: %v", err)
	}
	d1, err := st.GetDelivery(ctx, d1ID)
	if err != nil {
		t.Fatalf("GetDelivery d1: %v", err)
	}
	if d1.Status != DeliveryPending {
		t.Fatalf("status = %v, want pending", d1.Status)
	}
	if d1.Op != OpEditPending {
		t.Fatalf("op = %v, want edit_pending", d1.Op)
	}
	if d1.RemoteID != "7.7" {
		t.Fatalf("remote id = %q, want 7.7", d1.RemoteID)
	}
	if d1.Resolved() {
		t.Fatalf("d1 resolved, want unresolved (acked -1 != desired 1)")
	}

	if err := st.MarkEdited(ctx, d1ID, 1); err != nil {
		t.Fatalf("MarkEdited: %v", err)
	}
	d1, err = st.GetDelivery(ctx, d1ID)
	if err != nil {
		t.Fatalf("GetDelivery d1 after edit: %v", err)
	}
	if !d1.Resolved() {
		t.Fatalf("d1 not resolved after MarkEdited")
	}

	n, err = st.UnresolvedDeliveries(ctx, routeKey)
	if err != nil {
		t.Fatalf("UnresolvedDeliveries after resolve: %v", err)
	}
	if n != 0 {
		t.Fatalf("unresolved = %d, want 0", n)
	}

	if err := st.Resend(ctx, d0ID, "audit-nonfailed"); !errors.Is(err, ErrConflict) {
		t.Fatalf("Resend on non-failed row err = %v, want ErrConflict", err)
	}

	// A resolved row can never be failed; raise its desired revision first
	// (a later committed edit) so it is unresolved again.
	if err := st.MarkFailed(ctx, d0ID, "boom"); !errors.Is(err, ErrConflict) {
		t.Fatalf("MarkFailed on resolved row err = %v, want ErrConflict", err)
	}
	if err := st.SetDesired(ctx, d0ID, "revised", 2); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}
	if err := st.MarkFailed(ctx, d0ID, "boom"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	n, err = st.UnresolvedDeliveries(ctx, routeKey)
	if err != nil {
		t.Fatalf("UnresolvedDeliveries after fail: %v", err)
	}
	if n != 1 {
		t.Fatalf("unresolved = %d, want 1", n)
	}

	if err := st.Resend(ctx, d0ID, "retry"); err != nil {
		t.Fatalf("Resend: %v", err)
	}
	d0, err = st.GetDelivery(ctx, d0ID)
	if err != nil {
		t.Fatalf("GetDelivery d0 after resend: %v", err)
	}
	if d0.Status != DeliveryPending {
		t.Fatalf("status = %v, want pending", d0.Status)
	}
	if d0.Nonce != "" {
		t.Fatalf("nonce = %q, want empty", d0.Nonce)
	}

	list, err := st.ListDeliveries(ctx, 100)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("ListDeliveries len = %d, want 1 (only d0 unresolved)", len(list))
	}
	if list[0].ID != d0ID {
		t.Fatalf("ListDeliveries[0].ID = %d, want %d", list[0].ID, d0ID)
	}
}

// 10. PlanPreview creates the chunk-0 status row; MarkTerminal updates the
// same row rather than inserting a new one.
func TestPlanPreviewThenMarkTerminalUpdatesSameRow(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()
	route := slackRoute()

	d, err := st.Ingest(ctx, promptInbound(route, "pp1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	id := d.Item.ID
	if err := st.Transition(ctx, id, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if err := st.MarkAdmitted(ctx, id, "run-pp", "m1", "m2"); err != nil {
		t.Fatalf("MarkAdmitted: %v", err)
	}
	item, err := st.GetItem(ctx, id)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if item.RunID != "run-pp" {
		t.Fatalf("run id = %q, want run-pp", item.RunID)
	}

	preview, err := st.PlanPreview(ctx, item, "working...")
	if err != nil {
		t.Fatalf("PlanPreview: %v", err)
	}
	if preview.ChunkIndex != 0 {
		t.Fatalf("chunk = %d, want 0", preview.ChunkIndex)
	}
	if preview.DesiredRevision != 0 {
		t.Fatalf("revision = %d, want 0", preview.DesiredRevision)
	}
	if !preview.HasDesiredText || preview.DesiredText != "working..." {
		t.Fatalf("desired text = %q (has=%v), want working...", preview.DesiredText, preview.HasDesiredText)
	}
	if preview.Op != OpPlanned {
		t.Fatalf("preview op = %v, want planned", preview.Op)
	}

	plans := []DeliveryPlan{{ChunkIndex: 0, Text: "final answer", Revision: 1}}
	if err := st.MarkTerminal(ctx, id, "completed", CodeCompleted, plans); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	updated, err := st.DeliveryForRun(ctx, "run-pp", 0)
	if err != nil {
		t.Fatalf("DeliveryForRun: %v", err)
	}
	if updated.ID != preview.ID {
		t.Fatalf("delivery id changed: got %d, want %d (same row)", updated.ID, preview.ID)
	}
	if updated.DesiredRevision != 1 {
		t.Fatalf("desired revision = %d, want 1", updated.DesiredRevision)
	}
	if updated.DesiredText != "final answer" {
		t.Fatalf("desired text = %q, want %q", updated.DesiredText, "final answer")
	}
	if updated.Op != OpPlanned {
		t.Fatalf("op = %v, want planned (unchanged, remote_id still empty)", updated.Op)
	}
}

// A preview row acknowledged at revision 0 and then planned to revision 1
// must be visible to the scheduler scan after a restart.
func TestPlannedFinalAfterAckedPreviewIsSchedulable(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	in := Inbound{Route: Route{Platform: PlatformSlack, Installation: "T1", Channel: "D7", DMActor: "U1"}, MessageID: "7.1", Actor: "U1", ActorLabel: "U1", Kind: KindPrompt, Content: "x", ReceivedAt: time.Now()}
	d, err := st.Ingest(ctx, in, Capacity{MaxQueuedPerRoute: 8, MaxPendingGlobal: 256})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Transition(ctx, d.Item.ID, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkAdmitted(ctx, d.Item.ID, "run-7", "u", "a"); err != nil {
		t.Fatal(err)
	}
	item, _ := st.GetItem(ctx, d.Item.ID)
	row, err := st.PlanPreview(ctx, item, "Thinking…")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkCreateIntent(ctx, row.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkCreated(ctx, row.ID, "9.9", 0); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkTerminal(ctx, d.Item.ID, "completed", CodeCompleted, []DeliveryPlan{{ChunkIndex: 0, Text: "final", Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	got, _ := st.GetDelivery(ctx, row.ID)
	if got.Status != DeliveryPending || got.Op != OpEditPending || got.DesiredRevision != 1 || got.AckedRevision != 0 {
		t.Fatalf("row=%+v", got)
	}
	keys, err := st.RoutesWithWork(ctx, "", 10)
	if err != nil || len(keys) != 1 || keys[0] != in.Route.Key() {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	// Re-planning an older revision never lowers the row.
	if err := st.MarkEdited(ctx, row.ID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PlanPreview(ctx, item, "Thinking…"); err != nil {
		t.Fatal(err)
	}
	got, _ = st.GetDelivery(ctx, row.ID)
	if got.DesiredRevision != 1 || !got.Resolved() {
		t.Fatalf("revision lowered: %+v", got)
	}
	// A create intent can never be recorded again once a remote ID exists.
	if _, err := st.MarkCreateIntent(ctx, row.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("intent after create: %v", err)
	}
	if err := st.MarkAmbiguous(ctx, row.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("ambiguous after create: %v", err)
	}
}
