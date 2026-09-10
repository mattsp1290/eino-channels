package state

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	st, err := Open(context.Background(), dir, Options{SkipAgentDB: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func slackRoute() Route {
	return Route{Platform: PlatformSlack, Installation: "T1", Channel: "C1", ThreadRoot: "1.1"}
}

func slackDMRoute() Route {
	return Route{Platform: PlatformSlack, Installation: "T1", Channel: "D1", DMActor: "U1"}
}

func promptInbound(route Route, msgID, actor, content string) Inbound {
	return Inbound{Route: route, MessageID: msgID, Actor: actor, ActorLabel: actor, Kind: KindPrompt, Content: content, ReceivedAt: time.Now()}
}

func defaultCapacity() Capacity {
	return Capacity{MaxQueuedPerRoute: 8, MaxPendingGlobal: 256}
}

// 1. Route.Key format, and DedupKey/AdmissionKey determinism and shape.
func TestRouteKey(t *testing.T) {
	r := slackRoute()
	want := "v1|5:slack|2:T1|2:C1|3:1.1|0:"
	if got := r.Key(); got != want {
		t.Fatalf("Key() = %q, want %q", got, want)
	}

	rThread := r
	rThread.DMActor = ""
	rDM := r
	rDM.ThreadRoot = ""
	rDM.DMActor = "U1"
	if rThread.Key() == rDM.Key() {
		t.Fatalf("routes differing only in ThreadRoot vs DMActor produced the same key: %q", rThread.Key())
	}

	dm := slackDMRoute()
	if dm.Key() == r.Key() {
		t.Fatalf("DM route and shared route produced the same key")
	}
}

var admissionKeyPattern = regexp.MustCompile(`^[a-z0-9-]{68}$`)

func TestDedupAndAdmissionKeys(t *testing.T) {
	in := promptInbound(slackRoute(), "1.1", "U1", "hello")

	if got1, got2 := in.DedupKey(), in.DedupKey(); got1 != got2 {
		t.Fatalf("DedupKey not deterministic: %q vs %q", got1, got2)
	}

	admit1 := in.AdmissionKey()
	admit2 := in.AdmissionKey()
	if admit1 != admit2 {
		t.Fatalf("AdmissionKey not deterministic: %q vs %q", admit1, admit2)
	}
	if !strings.HasPrefix(admit1, "evt-") {
		t.Fatalf("AdmissionKey = %q, want evt- prefix", admit1)
	}
	if len(admit1) != 68 {
		t.Fatalf("AdmissionKey length = %d, want 68", len(admit1))
	}
	if !admissionKeyPattern.MatchString(admit1) {
		t.Fatalf("AdmissionKey %q does not match ^[a-z0-9-]{68}$", admit1)
	}
	if err := session.ValidateAdmissionKey(admit1); err != nil {
		t.Fatalf("session.ValidateAdmissionKey(%q): %v", admit1, err)
	}
}

// 2. Basic prompt ingestion: accepted, seq, generation, conversation
// creation, and dedup.
func TestIngestPromptAcceptedAndDuplicate(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()
	route := slackRoute()

	in1 := promptInbound(route, "1.1", "U1", "hello")
	d1, err := st.Ingest(ctx, in1, capacity)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if d1.Outcome != OutcomeAccepted {
		t.Fatalf("outcome = %v, want accepted", d1.Outcome)
	}
	if d1.Item.State != StateQueued {
		t.Fatalf("state = %v, want queued", d1.Item.State)
	}
	if d1.Item.Seq != 1 {
		t.Fatalf("seq = %d, want 1", d1.Item.Seq)
	}
	if d1.Item.Generation != 1 {
		t.Fatalf("generation = %d, want 1", d1.Item.Generation)
	}
	if d1.Conversation.CreatorActor != "U1" {
		t.Fatalf("creator = %q, want U1", d1.Conversation.CreatorActor)
	}
	if !strings.HasPrefix(d1.Conversation.RuntimeSessionID, "sess-") {
		t.Fatalf("runtime session id = %q, want sess- prefix", d1.Conversation.RuntimeSessionID)
	}

	in2 := promptInbound(route, "1.2", "U1", "world")
	d2, err := st.Ingest(ctx, in2, capacity)
	if err != nil {
		t.Fatalf("Ingest 2: %v", err)
	}
	if d2.Item.Seq != 2 {
		t.Fatalf("seq = %d, want 2", d2.Item.Seq)
	}

	d3, err := st.Ingest(ctx, in1, capacity)
	if err != nil {
		t.Fatalf("Ingest duplicate: %v", err)
	}
	if d3.Outcome != OutcomeDuplicate {
		t.Fatalf("outcome = %v, want duplicate", d3.Outcome)
	}
	if d3.Item.ID != d1.Item.ID {
		t.Fatalf("duplicate item id = %d, want %d", d3.Item.ID, d1.Item.ID)
	}

	n, err := st.CountByState(ctx, StateQueued)
	if err != nil {
		t.Fatalf("CountByState: %v", err)
	}
	if n != 2 {
		t.Fatalf("queued count = %d, want 2", n)
	}
}

// 3. Capacity enforcement, per-route and global.
func TestIngestCapacityPerRoute(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := Capacity{MaxQueuedPerRoute: 2, MaxPendingGlobal: 256}
	route := slackRoute()

	for i := 0; i < 3; i++ {
		in := promptInbound(route, fmt.Sprintf("m%d", i), "U1", fmt.Sprintf("body-%d", i))
		d, err := st.Ingest(ctx, in, capacity)
		if err != nil {
			t.Fatalf("Ingest %d: %v", i, err)
		}
		if d.Outcome != OutcomeAccepted {
			t.Fatalf("Ingest %d outcome = %v, want accepted", i, d.Outcome)
		}
	}

	overflowIn := promptInbound(route, "m-overflow", "U1", "overflow body")
	d4, err := st.Ingest(ctx, overflowIn, capacity)
	if err != nil {
		t.Fatalf("Ingest overflow: %v", err)
	}
	if d4.Outcome != OutcomeRejected {
		t.Fatalf("outcome = %v, want rejected", d4.Outcome)
	}
	if d4.Item.ResultCode != CodeOverflow {
		t.Fatalf("result code = %q, want %q", d4.Item.ResultCode, CodeOverflow)
	}

	got, err := st.GetItem(ctx, d4.Item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.Content != "" {
		t.Fatalf("content = %q, want empty", got.Content)
	}

	d5, err := st.Ingest(ctx, overflowIn, capacity)
	if err != nil {
		t.Fatalf("Ingest duplicate overflow: %v", err)
	}
	if d5.Outcome != OutcomeDuplicate {
		t.Fatalf("outcome = %v, want duplicate", d5.Outcome)
	}
	if d5.Item.ID != d4.Item.ID {
		t.Fatalf("duplicate id = %d, want %d", d5.Item.ID, d4.Item.ID)
	}
}

func TestIngestCapacityGlobal(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := Capacity{MaxQueuedPerRoute: 8, MaxPendingGlobal: 1}

	routeA := slackRoute()
	routeB := slackRoute()
	routeB.Channel = "C2"

	dA, err := st.Ingest(ctx, promptInbound(routeA, "a1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest A: %v", err)
	}
	if dA.Outcome != OutcomeAccepted {
		t.Fatalf("outcome A = %v, want accepted", dA.Outcome)
	}

	dB, err := st.Ingest(ctx, promptInbound(routeB, "b1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest B: %v", err)
	}
	if dB.Outcome != OutcomeRejected {
		t.Fatalf("outcome B = %v, want rejected", dB.Outcome)
	}
	if dB.Item.ResultCode != CodeOverflow {
		t.Fatalf("result code B = %q, want %q", dB.Item.ResultCode, CodeOverflow)
	}
}

// 4. RejectCode stores a rejected tombstone with the given code.
func TestIngestRejectCode(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()

	in := promptInbound(slackRoute(), "r1", "U1", "denied body")
	in.RejectCode = CodeDenied
	d, err := st.Ingest(ctx, in, capacity)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if d.Outcome != OutcomeRejected {
		t.Fatalf("outcome = %v, want rejected", d.Outcome)
	}
	if d.Item.State != StateRejected {
		t.Fatalf("state = %v, want rejected", d.Item.State)
	}
	if d.Item.ResultCode != CodeDenied {
		t.Fatalf("result code = %q, want %q", d.Item.ResultCode, CodeDenied)
	}

	got, err := st.GetItem(ctx, d.Item.ID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.Content != "" {
		t.Fatalf("content = %q, want empty", got.Content)
	}
}

// 5. Stop control: cutoff, cancellation, and target freezing.
func TestStopControl(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()
	route := slackRoute()

	d1, err := st.Ingest(ctx, promptInbound(route, "p1", "U1", "one"), capacity)
	if err != nil {
		t.Fatalf("Ingest p1: %v", err)
	}
	d2, err := st.Ingest(ctx, promptInbound(route, "p2", "U1", "two"), capacity)
	if err != nil {
		t.Fatalf("Ingest p2: %v", err)
	}

	stopIn := Inbound{Route: route, MessageID: "stop1", Actor: "U1", ActorLabel: "U1", Kind: KindStop, ReceivedAt: time.Now()}
	ds, err := st.Ingest(ctx, stopIn, capacity)
	if err != nil {
		t.Fatalf("Ingest stop: %v", err)
	}
	if ds.Outcome != OutcomeAccepted {
		t.Fatalf("stop outcome = %v, want accepted", ds.Outcome)
	}
	if ds.Item.State != StatePending {
		t.Fatalf("stop state = %v, want pending", ds.Item.State)
	}
	if ds.Item.StopCutoffSeq != 2 {
		t.Fatalf("stop cutoff = %d, want 2", ds.Item.StopCutoffSeq)
	}
	if ds.Item.StopTargetInboxID != 0 {
		t.Fatalf("stop target inbox id = %d, want 0", ds.Item.StopTargetInboxID)
	}

	got1, err := st.GetItem(ctx, d1.Item.ID)
	if err != nil {
		t.Fatalf("GetItem p1: %v", err)
	}
	if got1.State != StateCanceled {
		t.Fatalf("p1 state = %v, want canceled", got1.State)
	}
	got2, err := st.GetItem(ctx, d2.Item.ID)
	if err != nil {
		t.Fatalf("GetItem p2: %v", err)
	}
	if got2.State != StateCanceled {
		t.Fatalf("p2 state = %v, want canceled", got2.State)
	}

	// Bring the route back to admitted and issue a second stop targeting it.
	d3, err := st.Ingest(ctx, promptInbound(route, "p3", "U1", "three"), capacity)
	if err != nil {
		t.Fatalf("Ingest p3: %v", err)
	}
	if err := st.Transition(ctx, d3.Item.ID, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if err := st.MarkAdmitted(ctx, d3.Item.ID, "run-1", "m1", "m2"); err != nil {
		t.Fatalf("MarkAdmitted: %v", err)
	}

	stopIn2 := Inbound{Route: route, MessageID: "stop2", Actor: "U1", ActorLabel: "U1", Kind: KindStop, ReceivedAt: time.Now()}
	ds2, err := st.Ingest(ctx, stopIn2, capacity)
	if err != nil {
		t.Fatalf("Ingest stop2: %v", err)
	}
	if ds2.Item.StopTargetInboxID != d3.Item.ID {
		t.Fatalf("stop2 target inbox id = %d, want %d", ds2.Item.StopTargetInboxID, d3.Item.ID)
	}
	if ds2.Item.StopTargetRunID != "run-1" {
		t.Fatalf("stop2 target run id = %q, want run-1", ds2.Item.StopTargetRunID)
	}
}

// 6. New control: generation rotation on idle routes, and CodeBusy when the
// route has unfinished prompt work or unresolved deliveries.
func TestNewControl(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()
	route := slackRoute()

	newIn := Inbound{Route: route, MessageID: "new1", Actor: "U1", ActorLabel: "U1", Kind: KindNew, ReceivedAt: time.Now()}
	d, err := st.Ingest(ctx, newIn, capacity)
	if err != nil {
		t.Fatalf("Ingest new: %v", err)
	}
	if d.Outcome != OutcomeAccepted {
		t.Fatalf("outcome = %v, want accepted", d.Outcome)
	}
	if d.Item.State != StateComplete {
		t.Fatalf("state = %v, want complete", d.Item.State)
	}
	if d.Conversation.Generation != 2 {
		t.Fatalf("generation = %d, want 2", d.Conversation.Generation)
	}
	firstSessionID := d.Conversation.RuntimeSessionID

	initialConv, err := st.GetConversation(ctx, route)
	if err != nil {
		t.Fatalf("GetConversation: %v", err)
	}
	if initialConv.RuntimeSessionID != firstSessionID {
		t.Fatalf("conversation session id = %q, want %q", initialConv.RuntimeSessionID, firstSessionID)
	}

	// Busy route (queued prompt): New is rejected with CodeBusy.
	dq, err := st.Ingest(ctx, promptInbound(route, "p1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest prompt: %v", err)
	}
	if dq.Item.State != StateQueued {
		t.Fatalf("prompt state = %v, want queued", dq.Item.State)
	}

	newIn2 := Inbound{Route: route, MessageID: "new2", Actor: "U1", ActorLabel: "U1", Kind: KindNew, ReceivedAt: time.Now()}
	d2, err := st.Ingest(ctx, newIn2, capacity)
	if err != nil {
		t.Fatalf("Ingest new2: %v", err)
	}
	if d2.Outcome != OutcomeRejected {
		t.Fatalf("outcome = %v, want rejected", d2.Outcome)
	}
	if d2.Item.ResultCode != CodeBusy {
		t.Fatalf("result code = %q, want %q", d2.Item.ResultCode, CodeBusy)
	}

	convAfter, err := st.GetConversation(ctx, route)
	if err != nil {
		t.Fatalf("GetConversation: %v", err)
	}
	if convAfter.Generation != 2 {
		t.Fatalf("generation = %d, want unchanged 2", convAfter.Generation)
	}

	// Drive the prompt to terminal with an unresolved delivery, then confirm
	// New is still rejected busy.
	if err := st.Transition(ctx, dq.Item.ID, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if err := st.MarkAdmitted(ctx, dq.Item.ID, "run-new", "m1", "m2"); err != nil {
		t.Fatalf("MarkAdmitted: %v", err)
	}
	if err := st.MarkTerminal(ctx, dq.Item.ID, "completed", CodeCompleted, []DeliveryPlan{{ChunkIndex: 0, Text: "answer", Revision: 1}}); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	newIn3 := Inbound{Route: route, MessageID: "new3", Actor: "U1", ActorLabel: "U1", Kind: KindNew, ReceivedAt: time.Now()}
	d3, err := st.Ingest(ctx, newIn3, capacity)
	if err != nil {
		t.Fatalf("Ingest new3: %v", err)
	}
	if d3.Outcome != OutcomeRejected {
		t.Fatalf("outcome = %v, want rejected", d3.Outcome)
	}
	if d3.Item.ResultCode != CodeBusy {
		t.Fatalf("result code = %q, want %q", d3.Item.ResultCode, CodeBusy)
	}
}

// 7. Help control completes immediately.
func TestHelpControl(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()

	in := Inbound{Route: slackRoute(), MessageID: "help1", Actor: "U1", ActorLabel: "U1", Kind: KindHelp, ReceivedAt: time.Now()}
	d, err := st.Ingest(ctx, in, capacity)
	if err != nil {
		t.Fatalf("Ingest help: %v", err)
	}
	if d.Outcome != OutcomeAccepted {
		t.Fatalf("outcome = %v, want accepted", d.Outcome)
	}
	if d.Item.State != StateComplete {
		t.Fatalf("state = %v, want complete", d.Item.State)
	}
}

// 8. Transition/MarkAdmitted/MarkTerminal guards, and MarkTerminal's
// delivery planning and idempotency.
func TestTransitionGuards(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()
	route := slackRoute()

	d, err := st.Ingest(ctx, promptInbound(route, "g1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	id := d.Item.ID

	if err := st.Transition(ctx, id, StateAdmitting, StateAdmitted, ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("Transition with wrong from err = %v, want ErrConflict", err)
	}

	if err := st.MarkAdmitted(ctx, id, "run-g", "m1", "m2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("MarkAdmitted on queued item err = %v, want ErrConflict", err)
	}

	if err := st.MarkTerminal(ctx, id, "completed", CodeCompleted, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("MarkTerminal on queued item err = %v, want ErrConflict", err)
	}

	if err := st.Transition(ctx, id, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatalf("Transition to admitting: %v", err)
	}
	if err := st.MarkAdmitted(ctx, id, "run-g", "m1", "m2"); err != nil {
		t.Fatalf("MarkAdmitted: %v", err)
	}

	plans := []DeliveryPlan{{ChunkIndex: 0, Text: "a", Revision: 1}, {ChunkIndex: 1, Text: "b", Revision: 1}}
	if err := st.MarkTerminal(ctx, id, "completed", CodeCompleted, plans); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	got, err := st.GetItem(ctx, id)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.State != StateTerminal {
		t.Fatalf("state = %v, want terminal", got.State)
	}
	if got.Content != "" {
		t.Fatalf("content = %q, want empty", got.Content)
	}

	nd, blocked, err := st.NextDelivery(ctx, route.Key())
	if err != nil {
		t.Fatalf("NextDelivery: %v", err)
	}
	if blocked {
		t.Fatalf("blocked = true, want false")
	}
	if nd.ChunkIndex != 0 {
		t.Fatalf("next delivery chunk = %d, want 0", nd.ChunkIndex)
	}

	dl1, err := st.DeliveryForRun(ctx, "run-g", 1)
	if err != nil {
		t.Fatalf("DeliveryForRun chunk1: %v", err)
	}
	if dl1.ChunkIndex != 1 {
		t.Fatalf("chunk = %d, want 1", dl1.ChunkIndex)
	}

	// MarkTerminal is idempotent.
	if err := st.MarkTerminal(ctx, id, "completed", CodeCompleted, plans); err != nil {
		t.Fatalf("second MarkTerminal: %v", err)
	}
	n, err := st.UnresolvedDeliveries(ctx, route.Key())
	if err != nil {
		t.Fatalf("UnresolvedDeliveries: %v", err)
	}
	if n != 2 {
		t.Fatalf("unresolved deliveries = %d, want 2", n)
	}
}

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

// 11. Thread bindings are idempotent on first-writer-wins.
func TestThreadBindings(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	got1, err := st.BindThread(ctx, PlatformSlack, "T1", "1.1", "thread-a")
	if err != nil {
		t.Fatalf("BindThread: %v", err)
	}
	if got1 != "thread-a" {
		t.Fatalf("bound = %q, want thread-a", got1)
	}

	got2, err := st.BindThread(ctx, PlatformSlack, "T1", "1.1", "thread-b")
	if err != nil {
		t.Fatalf("BindThread second: %v", err)
	}
	if got2 != "thread-a" {
		t.Fatalf("bound = %q, want thread-a (first bound wins)", got2)
	}

	found, err := st.LookupThread(ctx, PlatformSlack, "T1", "1.1")
	if err != nil {
		t.Fatalf("LookupThread: %v", err)
	}
	if found != "thread-a" {
		t.Fatalf("found = %q, want thread-a", found)
	}

	_, err = st.LookupThread(ctx, PlatformSlack, "T1", "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupThread missing err = %v, want ErrNotFound", err)
	}
}

// 12. RoutesWithWork, PendingStops, RecoveryItems, ActiveItem.
func TestRoutesWithWork(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()

	routeA := slackRoute()
	routeA.Channel = "CA"
	routeB := slackRoute()
	routeB.Channel = "CB"
	routeC := slackRoute()
	routeC.Channel = "CC" // stays idle: never ingested

	dA, err := st.Ingest(ctx, promptInbound(routeA, "a1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest A: %v", err)
	}
	if dA.Item.State != StateQueued {
		t.Fatalf("A state = %v, want queued", dA.Item.State)
	}

	dB, err := st.Ingest(ctx, promptInbound(routeB, "b1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest B: %v", err)
	}
	if err := st.Transition(ctx, dB.Item.ID, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatalf("Transition B: %v", err)
	}
	if err := st.MarkAdmitted(ctx, dB.Item.ID, "run-b", "m1", "m2"); err != nil {
		t.Fatalf("MarkAdmitted B: %v", err)
	}
	if err := st.MarkTerminal(ctx, dB.Item.ID, "completed", CodeCompleted, []DeliveryPlan{{ChunkIndex: 0, Text: "answer", Revision: 1}}); err != nil {
		t.Fatalf("MarkTerminal B: %v", err)
	}

	keys, err := st.RoutesWithWork(ctx, "", 10)
	if err != nil {
		t.Fatalf("RoutesWithWork: %v", err)
	}
	set := map[string]bool{}
	for _, k := range keys {
		set[k] = true
	}
	if !set[routeA.Key()] {
		t.Fatalf("routeA missing from %v", keys)
	}
	if !set[routeB.Key()] {
		t.Fatalf("routeB missing from %v", keys)
	}
	if set[routeC.Key()] {
		t.Fatalf("idle routeC present in %v", keys)
	}
}

func TestPendingStopsRecoveryActiveItem(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := defaultCapacity()
	route := slackRoute()

	if _, err := st.ActiveItem(ctx, route.Key()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ActiveItem on idle route err = %v, want ErrNotFound", err)
	}

	stopIn := Inbound{Route: route, MessageID: "s1", Actor: "U1", ActorLabel: "U1", Kind: KindStop, ReceivedAt: time.Now()}
	ds, err := st.Ingest(ctx, stopIn, capacity)
	if err != nil {
		t.Fatalf("Ingest stop: %v", err)
	}

	// A stop with nothing running or queued completes on arrival.
	if !ds.StopNoop || ds.Item.State != StateComplete {
		t.Fatalf("idle stop = %+v", ds)
	}
	if stops, err := st.PendingStops(ctx, route.Key()); err != nil || len(stops) != 0 {
		t.Fatalf("PendingStops = %+v err = %v, want none", stops, err)
	}

	d, err := st.Ingest(ctx, promptInbound(route, "p1", "U1", "hi"), capacity)
	if err != nil {
		t.Fatalf("Ingest prompt: %v", err)
	}
	if err := st.Transition(ctx, d.Item.ID, StateQueued, StateAdmitting, ""); err != nil {
		t.Fatalf("Transition: %v", err)
	}

	recovering, err := st.RecoveryItems(ctx, 10)
	if err != nil {
		t.Fatalf("RecoveryItems: %v", err)
	}
	if !containsItemID(recovering, d.Item.ID) {
		t.Fatalf("RecoveryItems missing admitting item %d: %+v", d.Item.ID, recovering)
	}

	active, err := st.ActiveItem(ctx, route.Key())
	if err != nil {
		t.Fatalf("ActiveItem: %v", err)
	}
	if active.ID != d.Item.ID {
		t.Fatalf("active id = %d, want %d", active.ID, d.Item.ID)
	}

	if err := st.MarkAdmitted(ctx, d.Item.ID, "run-rec", "m1", "m2"); err != nil {
		t.Fatalf("MarkAdmitted: %v", err)
	}
	recovering2, err := st.RecoveryItems(ctx, 10)
	if err != nil {
		t.Fatalf("RecoveryItems 2: %v", err)
	}
	if !containsItemID(recovering2, d.Item.ID) {
		t.Fatalf("RecoveryItems missing admitted item %d: %+v", d.Item.ID, recovering2)
	}

	active2, err := st.ActiveItem(ctx, route.Key())
	if err != nil {
		t.Fatalf("ActiveItem 2: %v", err)
	}
	if active2.ID != d.Item.ID {
		t.Fatalf("active2 id = %d, want %d", active2.ID, d.Item.ID)
	}
}

func containsItemID(items []Item, id int64) bool {
	for _, it := range items {
		if it.ID == id {
			return true
		}
	}
	return false
}

// 13. Concurrent ingestion respects per-route capacity exactly, under -race.
func TestIngestConcurrentCapacityPerRoute(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	capacity := Capacity{MaxQueuedPerRoute: 5, MaxPendingGlobal: 1000}
	route := slackRoute()

	const n = 20
	var wg sync.WaitGroup
	dispositions := make([]Disposition, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := promptInbound(route, fmt.Sprintf("c%d", i), "U1", fmt.Sprintf("body-%d", i))
			d, err := st.Ingest(ctx, in, capacity)
			dispositions[i] = d
			errs[i] = err
		}(i)
	}
	wg.Wait()

	var accepted, rejected int
	seqSeen := map[int64]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Ingest goroutine %d: %v", i, err)
		}
		switch dispositions[i].Outcome {
		case OutcomeAccepted:
			accepted++
			seq := dispositions[i].Item.Seq
			if seqSeen[seq] {
				t.Fatalf("duplicate seq %d among accepted items", seq)
			}
			seqSeen[seq] = true
		case OutcomeRejected:
			rejected++
		default:
			t.Fatalf("unexpected outcome %v for goroutine %d", dispositions[i].Outcome, i)
		}
	}

	// perRoute counts existing queued/admitting/admitted rows and rejects
	// when perRoute > MaxQueuedPerRoute, so perRoute of 0..5 (6 values) are
	// all accepted before the 7th (perRoute == 6) is rejected.
	const wantAccepted = 6
	if accepted != wantAccepted {
		t.Fatalf("accepted = %d, want %d", accepted, wantAccepted)
	}
	if rejected != n-wantAccepted {
		t.Fatalf("rejected = %d, want %d", rejected, n-wantAccepted)
	}

	got, err := st.CountByState(ctx, StateQueued)
	if err != nil {
		t.Fatalf("CountByState: %v", err)
	}
	if got != wantAccepted {
		t.Fatalf("queued count = %d, want %d", got, wantAccepted)
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
