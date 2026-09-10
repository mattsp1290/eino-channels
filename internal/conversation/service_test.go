package conversation_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	einoschema "github.com/cloudwego/eino/schema"

	"github.com/mattsp1290/eino-agent/session"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/state"
	"github.com/mattsp1290/eino-channels/internal/testkit"
)

const wait = 20 * time.Second

func countRole(msgs []*einoschema.Message, role einoschema.RoleType) int {
	n := 0
	for _, m := range msgs {
		if m.Role == role {
			n++
		}
	}
	return n
}

func TestTwoTurnsThenReopenSeesHistoryOnce(t *testing.T) {
	dir := t.TempDir()
	script := testkit.NewScript()
	env := testkit.Open(t, testkit.Options{Dir: dir, Script: script, Started: true})
	defer env.Close()

	r1 := env.Ingest(testkit.DM("D1", "U1", "1.000001", "first"))
	if r1.Outcome != state.OutcomeAccepted {
		t.Fatalf("outcome=%v", r1.Outcome)
	}
	texts := env.WaitDelivered(r1.Item.ID, wait)
	if len(texts) != 1 || texts[0] != "reply:first" {
		t.Fatalf("delivered=%q", texts)
	}
	r2 := env.Ingest(testkit.DM("D1", "U1", "1.000002", "second"))
	texts = env.WaitDelivered(r2.Item.ID, wait)
	if texts[0] != "reply:second" {
		t.Fatalf("delivered=%q", texts)
	}
	reqs := script.Requests()
	if len(reqs) != 2 || countRole(reqs[1].Messages, einoschema.User) != 2 || countRole(reqs[1].Messages, einoschema.Assistant) != 1 {
		t.Fatalf("history after second turn: %d requests, msgs=%d", len(reqs), len(reqs[1].Messages))
	}
	if reqs[1].Messages[len(reqs[1].Messages)-1].Content != "second" {
		t.Fatalf("current message not last: %+v", reqs[1].Messages)
	}
	if strings.Contains(reqs[0].Messages[0].Content, "[U1]") {
		t.Fatal("DM prompts must not carry participant labels")
	}
	env.Close()

	// Reopen: the third turn observes both previous pairs exactly once.
	env = testkit.Open(t, testkit.Options{Dir: dir, Script: script, Started: true})
	defer env.Close()
	r3 := env.Ingest(testkit.DM("D1", "U1", "1.000003", "third"))
	texts = env.WaitDelivered(r3.Item.ID, wait)
	if texts[0] != "reply:third" {
		t.Fatalf("delivered=%q", texts)
	}
	reqs = script.Requests()
	last := reqs[len(reqs)-1]
	if len(reqs) != 3 || countRole(last.Messages, einoschema.User) != 3 || countRole(last.Messages, einoschema.Assistant) != 2 {
		t.Fatalf("history after reopen: requests=%d msgs=%d", len(reqs), len(last.Messages))
	}
	conv, err := env.Store.GetConversation(context.Background(), state.Route{Platform: state.PlatformSlack, Installation: "T1", Channel: "D1", DMActor: "U1"})
	if err != nil || conv.Generation != 1 {
		t.Fatalf("conv=%+v err=%v", conv, err)
	}
	// The provider session header identity is stable and opaque.
	if last.Identity.SessionID != conv.RuntimeSessionID || !strings.HasPrefix(agentbridge.ProviderSessionID(last.Identity.SessionID), "ecs-") {
		t.Fatalf("identity=%+v", last.Identity)
	}
}

func TestDuplicateEventsAdmitOnce(t *testing.T) {
	script := testkit.NewScript()
	env := testkit.Open(t, testkit.Options{Script: script, Started: true})
	defer env.Close()
	in := testkit.Inbound("C1", "9.000001", "U1", "9.000001", "hello")
	r1 := env.Ingest(in)
	env.WaitDelivered(r1.Item.ID, wait)
	dup := env.Ingest(in)
	if dup.Outcome != state.OutcomeDuplicate || dup.Item.ID != r1.Item.ID {
		t.Fatalf("dup=%+v", dup)
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(script.Requests()); n != 1 {
		t.Fatalf("provider requests=%d", n)
	}
	// Same text with a new platform ID is a genuinely new turn.
	in2 := in
	in2.MessageID = "9.000002"
	r2 := env.Ingest(in2)
	env.WaitDelivered(r2.Item.ID, wait)
	if n := len(script.Requests()); n != 2 {
		t.Fatalf("provider requests=%d", n)
	}
	// Shared-thread prompts carry the participant label.
	if !strings.HasPrefix(script.Requests()[0].Messages[0].Content, "[U1] ") {
		t.Fatalf("label missing: %q", script.Requests()[0].Messages[0].Content)
	}
	msgs := env.Deliverer.Messages()
	if len(msgs) != 2 || msgs[0] != "reply:[U1] hello" {
		t.Fatalf("messages=%q", msgs)
	}
}

func TestStopInterruptsAndKeepsPartial(t *testing.T) {
	script := testkit.NewScript()
	started, block := make(chan struct{}), make(chan struct{})
	script.Push(testkit.Behavior{Reply: []string{"partial ", "never"}, Started: started, Block: block})
	env := testkit.Open(t, testkit.Options{Script: script, Started: true})
	defer env.Close()
	r1 := env.Ingest(testkit.DM("D1", "U1", "2.000001", "go"))
	<-started
	testkit.Eventually(t, wait, func() bool { return strings.TrimSpace(env.Deliverer.Text("m1")) == "partial" }, "partial preview visible")
	// A queued prompt behind the running one is canceled by stop.
	queued := env.Ingest(testkit.DM("D1", "U1", "2.000002", "queued"))
	stop := env.Ingest(testkit.DM("D1", "U1", "2.000003", "!stop"))
	if stop.Outcome != state.OutcomeAccepted || stop.Item.Kind != state.KindStop {
		t.Fatalf("stop=%+v", stop)
	}
	texts := env.WaitDelivered(r1.Item.ID, wait)
	if texts[0] != "partial"+conversation.TextStopped {
		t.Fatalf("delivered=%q", texts)
	}
	testkit.Eventually(t, wait, func() bool {
		it, _ := env.Store.GetItem(context.Background(), queued.Item.ID)
		return it.State == state.StateCanceled
	}, "queued canceled")
	testkit.Eventually(t, wait, func() bool {
		it, _ := env.Store.GetItem(context.Background(), stop.Item.ID)
		return it.State == state.StateComplete
	}, "stop complete")
	if n := len(script.Requests()); n != 1 {
		t.Fatalf("provider requests=%d", n)
	}
	// Follow-up works; the admitted user text is in history. The runtime
	// commits no partial assistant text on interruption, so only the live
	// prefix was shown (labeled stopped) and the model history has no
	// assistant text for that turn.
	r3 := env.Ingest(testkit.DM("D1", "U1", "2.000004", "after"))
	env.WaitDelivered(r3.Item.ID, wait)
	reqs := script.Requests()
	if countRole(reqs[1].Messages, einoschema.User) != 2 || reqs[1].Messages[len(reqs[1].Messages)-1].Content != "after" {
		t.Fatalf("history=%+v", reqs[1].Messages)
	}
	found := false
	for _, n := range env.Deliverer.Notices() {
		if n == conversation.NoticeStopped {
			found = true
		}
	}
	if !found {
		t.Fatalf("notices=%q", env.Deliverer.Notices())
	}
}

func TestNewRotatesOnlyWhenIdle(t *testing.T) {
	script := testkit.NewScript()
	started, block := make(chan struct{}), make(chan struct{})
	script.Push(testkit.Behavior{Reply: []string{"a"}, Started: started, Block: block})
	env := testkit.Open(t, testkit.Options{Script: script, Started: true})
	defer env.Close()
	r1 := env.Ingest(testkit.Inbound("C1", "3.000001", "U1", "3.000001", "one"))
	<-started
	busy := env.Ingest(testkit.Inbound("C1", "3.000001", "U1", "3.000002", "!new"))
	if busy.Outcome != state.OutcomeRejected || busy.Notice != conversation.NoticeBusyNew {
		t.Fatalf("busy=%+v", busy)
	}
	close(block)
	env.WaitDelivered(r1.Item.ID, wait)
	// Another participant may not reset a shared conversation.
	denied := env.Ingest(testkit.Inbound("C1", "3.000001", "U2", "3.000003", "!new"))
	if denied.Outcome != state.OutcomeRejected || denied.Notice != conversation.NoticeDenied {
		t.Fatalf("denied=%+v", denied)
	}
	ok := env.Ingest(testkit.Inbound("C1", "3.000001", "U1", "3.000004", "!new"))
	if ok.Outcome != state.OutcomeAccepted || ok.Notice != conversation.NoticeNewStarted {
		t.Fatalf("new=%+v", ok)
	}
	r2 := env.Ingest(testkit.Inbound("C1", "3.000001", "U2", "3.000005", "two"))
	env.WaitDelivered(r2.Item.ID, wait)
	reqs := script.Requests()
	if len(reqs) != 2 || len(reqs[1].Messages) != 1 {
		t.Fatalf("fresh generation history=%d", len(reqs[1].Messages))
	}
	if reqs[1].Identity.SessionID == reqs[0].Identity.SessionID {
		t.Fatal("generation rotation must change the runtime session")
	}
	help := env.Ingest(testkit.Inbound("C1", "3.000001", "U2", "3.000006", "!help"))
	if help.Notice != conversation.NoticeHelp {
		t.Fatalf("help=%+v", help)
	}
	// Literal text that is not exactly a command is conversation input.
	r3 := env.Ingest(testkit.Inbound("C1", "3.000001", "U1", "3.000007", "!newish idea"))
	if r3.Item.Kind != state.KindPrompt {
		t.Fatalf("kind=%s", r3.Item.Kind)
	}
	env.WaitDelivered(r3.Item.ID, wait)
}

func TestQueueOrderAndOverflow(t *testing.T) {
	script := testkit.NewScript()
	started, block := make(chan struct{}), make(chan struct{})
	script.Push(testkit.Behavior{Reply: []string{"first"}, Started: started, Block: block})
	limits := testkitLimits()
	limits.MaxQueuedPerConversation = 2
	env := testkit.Open(t, testkit.Options{Script: script, Started: true, Limits: limits})
	defer env.Close()
	r1 := env.Ingest(testkit.DM("D1", "U1", "4.000001", "p1"))
	<-started
	r2 := env.Ingest(testkit.DM("D1", "U1", "4.000002", "p2"))
	r3 := env.Ingest(testkit.DM("D1", "U1", "4.000003", "p3"))
	over := env.Ingest(testkit.DM("D1", "U1", "4.000004", "p4"))
	if over.Outcome != state.OutcomeRejected || over.Notice != conversation.NoticeOverflow {
		t.Fatalf("overflow=%+v", over)
	}
	close(block)
	env.WaitDelivered(r1.Item.ID, wait)
	env.WaitDelivered(r2.Item.ID, wait)
	env.WaitDelivered(r3.Item.ID, wait)
	msgs := env.Deliverer.Messages()
	if len(msgs) != 3 || msgs[0] != "first" || msgs[1] != "reply:p2" || msgs[2] != "reply:p3" {
		t.Fatalf("order=%q", msgs)
	}
	if n := len(script.Requests()); n != 3 {
		t.Fatalf("requests=%d", n)
	}
	// Oversize prompt is rejected before any provider request.
	big := env.Ingest(testkit.DM("D1", "U1", "4.000005", strings.Repeat("x", 17<<10)))
	if big.Outcome != state.OutcomeRejected || big.Notice != conversation.NoticeOversize {
		t.Fatalf("oversize=%+v", big)
	}
}

func testkitLimits() config.Limits { return config.DefaultLimits() }

func TestCrashBeforeReceiptPersistenceRecoversWithoutSecondInference(t *testing.T) {
	dir := t.TempDir()
	script := testkit.NewScript()
	// Process A: ingest and admit, then die before persisting the receipt.
	env := testkit.Open(t, testkit.Options{Dir: dir, Script: script})
	r := env.Ingest(testkit.DM("D1", "U1", "5.000001", "crash"))
	if err := env.Store.Transition(context.Background(), r.Item.ID, state.StateQueued, state.StateAdmitting, ""); err != nil {
		t.Fatal(err)
	}
	conv, _ := env.Store.GetConversationByKey(context.Background(), r.Item.RouteKey)
	item, _ := env.Store.GetItem(context.Background(), r.Item.ID)
	res, err := env.Bridge.Submit(context.Background(), agentbridge.Turn{SessionID: session.ID(conv.RuntimeSessionID), AdmissionKey: item.AdmissionKey, Content: item.Content, WorkspaceID: conv.Route.Key()})
	if err != nil || res.Handle == nil {
		t.Fatalf("submit: %+v %v", res, err)
	}
	<-res.Handle.Done()
	env.Close()

	// Process B: recovery associates the original run and delivers once.
	env = testkit.Open(t, testkit.Options{Dir: dir, Script: script, Started: true})
	defer env.Close()
	texts := env.WaitDelivered(r.Item.ID, wait)
	if texts[0] != "reply:crash" {
		t.Fatalf("delivered=%q", texts)
	}
	if n := len(script.Requests()); n != 1 {
		t.Fatalf("provider requests=%d (must be exactly one)", n)
	}
	it, _ := env.Store.GetItem(context.Background(), r.Item.ID)
	if it.RunID != string(res.Receipt.RunID) {
		t.Fatalf("run association: %q vs %q", it.RunID, res.Receipt.RunID)
	}
	// A duplicate of the crashed event resolves the same run.
	dup := env.Ingest(testkit.DM("D1", "U1", "5.000001", "crash"))
	if dup.Outcome != state.OutcomeDuplicate {
		t.Fatalf("dup=%+v", dup)
	}
}

func TestKillDuringStreamRecoversInterruptedWithoutReplay(t *testing.T) {
	dir := t.TempDir()
	script := testkit.NewScript()
	started, block := make(chan struct{}), make(chan struct{})
	script.Push(testkit.Behavior{Reply: []string{"partial", "rest"}, Started: started, Block: block})
	// Process A: admits and streams, then is killed with the run still leased.
	env := testkit.Open(t, testkit.Options{Dir: dir, Script: script})
	r := env.Ingest(testkit.DM("D1", "U1", "6.000001", "stream"))
	if err := env.Store.Transition(context.Background(), r.Item.ID, state.StateQueued, state.StateAdmitting, ""); err != nil {
		t.Fatal(err)
	}
	conv, _ := env.Store.GetConversationByKey(context.Background(), r.Item.RouteKey)
	item, _ := env.Store.GetItem(context.Background(), r.Item.ID)
	res, err := env.Bridge.Submit(context.Background(), agentbridge.Turn{SessionID: session.ID(conv.RuntimeSessionID), AdmissionKey: item.AdmissionKey, Content: item.Content, WorkspaceID: conv.Route.Key()})
	if err != nil || res.Handle == nil {
		t.Fatalf("submit: %+v %v", res, err)
	}
	if err := env.Store.MarkAdmitted(context.Background(), r.Item.ID, string(res.Receipt.RunID), string(res.Receipt.UserMessageID), string(res.Receipt.AssistantMessageID)); err != nil {
		t.Fatal(err)
	}
	<-started
	time.Sleep(200 * time.Millisecond) // let the partial delta commit
	_ = env.Bridge.Close(context.Background())
	_ = env.Store.Close() // simulated crash: no interrupt, no settlement
	env.Store = nil

	// Process B: waits for lease expiry, resumes, settles interrupted.
	env2 := testkit.Open(t, testkit.Options{Dir: dir, Script: script, Started: true})
	defer env2.Close()
	texts := env2.WaitDelivered(r.Item.ID, 30*time.Second)
	if !strings.Contains(texts[0], "interrupted by a service restart") {
		t.Fatalf("delivered=%q", texts)
	}
	if n := len(script.Requests()); n != 1 {
		t.Fatalf("provider requests=%d (zero replayed inference required)", n)
	}
	close(block)
	r2 := env2.Ingest(testkit.DM("D1", "U1", "6.000002", "next"))
	env2.WaitDelivered(r2.Item.ID, wait)
}

func TestAmbiguousDeliveryBlocksSuccessorsUntilOperatorResolution(t *testing.T) {
	dir := t.TempDir()
	script := testkit.NewScript()
	d := testkit.NewDeliverer(3500)
	d.FailCreate = func(_ state.Destination, text string) error {
		return &conversation.DeliveryError{Kind: conversation.KindAmbiguous, Err: errors.New("timeout")}
	}
	env := testkit.Open(t, testkit.Options{Dir: dir, Script: script, Deliverer: d, Started: true})
	a := env.Ingest(testkit.DM("D1", "U1", "7.000001", "A"))
	testkit.Eventually(t, wait, func() bool {
		it, _ := env.Store.GetItem(context.Background(), a.Item.ID)
		if it.State != state.StateTerminal {
			return false
		}
		row, blocked, err := env.Store.NextDelivery(context.Background(), it.RouteKey)
		return err == nil && blocked && row.Status == state.DeliveryAmbiguous
	}, "A ambiguous")
	d.FailCreate = nil
	b := env.Ingest(testkit.DM("D1", "U1", "7.000002", "B"))
	testkit.Eventually(t, wait, func() bool {
		it, _ := env.Store.GetItem(context.Background(), b.Item.ID)
		return it.State == state.StateTerminal
	}, "B terminal")
	if n := len(script.Requests()); n != 2 {
		t.Fatalf("requests=%d", n)
	}
	// Nothing was created for B while A is unresolved.
	for _, c := range d.Calls() {
		if c.Op == "create" {
			t.Fatalf("unexpected create: %+v", c)
		}
	}
	rej := env.Ingest(testkit.DM("D1", "U1", "7.000003", "!new"))
	if rej.Outcome != state.OutcomeRejected || rej.Notice != conversation.NoticeBusyNew {
		t.Fatalf("new=%+v", rej)
	}
	env.Close()

	// Operator associates the message that actually landed, then restarts.
	st, err := state.Open(context.Background(), dir, state.Options{SkipAgentDB: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := st.ListDeliveries(context.Background(), 10)
	if len(rows) != 2 || rows[0].Status != state.DeliveryAmbiguous {
		t.Fatalf("rows=%+v", rows)
	}
	remote, _ := d.Create(context.Background(), rows[0].Destination, "Thinking…", "")
	if err := st.AssociateMessage(context.Background(), rows[0].ID, remote, "operator"); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	env = testkit.Open(t, testkit.Options{Dir: dir, Script: script, Deliverer: d, Started: true})
	defer env.Close()
	ta := env.WaitDelivered(a.Item.ID, wait)
	tb := env.WaitDelivered(b.Item.ID, wait)
	if ta[0] != "reply:A" || tb[0] != "reply:B" {
		t.Fatalf("A=%q B=%q", ta, tb)
	}
	// Order: A's edit precedes B's create.
	var seq []string
	for _, c := range d.Calls() {
		if c.Op == "edit" || c.Op == "create" {
			seq = append(seq, c.Op+":"+c.Text)
		}
	}
	joined := strings.Join(seq, ",")
	if strings.Index(joined, "edit:reply:A") > strings.Index(joined, "create:reply:B") {
		t.Fatalf("order=%v", seq)
	}
	if n := len(script.Requests()); n != 2 {
		t.Fatalf("regenerated answer: requests=%d", n)
	}
	okNew := env.Ingest(testkit.DM("D1", "U1", "7.000004", "!new"))
	if okNew.Outcome != state.OutcomeAccepted {
		t.Fatalf("new after resolution=%+v", okNew)
	}
}

func TestOutputCapInterruptsWithNotice(t *testing.T) {
	script := testkit.NewScript()
	parts := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		parts = append(parts, strings.Repeat("y", 1024))
	}
	script.Push(testkit.Behavior{Reply: parts})
	env := testkit.Open(t, testkit.Options{Script: script, Started: true})
	defer env.Close()
	r := env.Ingest(testkit.DM("D1", "U1", "8.000001", "long"))
	texts := env.WaitDelivered(r.Item.ID, wait)
	joined := strings.Join(texts, "")
	if !strings.HasSuffix(joined, strings.TrimSpace(conversation.TextOutputLimit)) || len(joined) > 32<<10+len(conversation.TextOutputLimit)+16 {
		t.Fatalf("expected capped text with limit notice, got %d bytes ending %q", len(joined), joined[max(0, len(joined)-40):])
	}
	it, _ := env.Store.GetItem(context.Background(), r.Item.ID)
	if it.ResultCode != state.CodeOutputLimit {
		t.Fatalf("code=%s", it.ResultCode)
	}
	for _, c := range texts {
		if len(c) > 3600 {
			t.Fatalf("chunk too large: %d", len(c))
		}
	}
}

func TestProviderFailureIsSafeAndSecretFree(t *testing.T) {
	script := testkit.NewScript()
	script.Push(testkit.Behavior{Err: errors.New("SENTINEL_SECRET_BODY unauthorized")})
	script.Push(testkit.Behavior{ToolCall: true})
	env := testkit.Open(t, testkit.Options{Script: script, Started: true})
	defer env.Close()
	r := env.Ingest(testkit.DM("D1", "U1", "9.000001", "fail"))
	texts := env.WaitDelivered(r.Item.ID, wait)
	if texts[0] != conversation.TextFailed {
		t.Fatalf("delivered=%q", texts)
	}
	r2 := env.Ingest(testkit.DM("D1", "U1", "9.000002", "tool"))
	texts = env.WaitDelivered(r2.Item.ID, wait)
	if texts[0] != conversation.TextFailed {
		t.Fatalf("tool-call delivered=%q", texts)
	}
	all := env.Logs.String()
	for _, c := range env.Deliverer.Calls() {
		all += c.Text
	}
	if strings.Contains(all, "SENTINEL_SECRET") {
		t.Fatal("secret sentinel leaked into logs or chat")
	}
}

func TestPreviewEditsThenFinal(t *testing.T) {
	script := testkit.NewScript()
	started, block := make(chan struct{}), make(chan struct{})
	script.Push(testkit.Behavior{Reply: []string{"hello ", "world"}, Started: started, Block: block})
	env := testkit.Open(t, testkit.Options{Script: script, Started: true})
	defer env.Close()
	r := env.Ingest(testkit.DM("D1", "U1", "10.000001", "hi"))
	<-started
	testkit.Eventually(t, wait, func() bool {
		return strings.TrimSpace(env.Deliverer.Text("m1")) == "hello"
	}, "preview edit visible before completion")
	close(block)
	texts := env.WaitDelivered(r.Item.ID, wait)
	if texts[0] != "hello world" || env.Deliverer.Text("m1") != "hello world" {
		t.Fatalf("final=%q", texts)
	}
	var creates int
	for _, c := range env.Deliverer.Calls() {
		if c.Op == "create" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("creates=%d", creates)
	}
}
