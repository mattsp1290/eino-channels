package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/state"
	"github.com/mattsp1290/eino-channels/internal/testkit"
)

// Gate 1: real wire, two turns, teardown/reopen, third turn sees both pairs.
func TestRealProviderTwoTurnsReopenAndIsolation(t *testing.T) {
	dir := t.TempDir()
	provider := newFakeOpenCode(t)
	env := openReal(t, dir, provider, config.DefaultLimits())

	r1 := env.ingest(testkit.DM("D1", "U1", "1.000001", "one"))
	if got := env.waitDelivered(env.slack, r1.Item.ID); got[0] != "reply:one" {
		t.Fatalf("delivered=%q", got)
	}
	r2 := env.ingest(testkit.DM("D1", "U1", "1.000002", "two"))
	env.waitDelivered(env.slack, r2.Item.ID)
	reqs := provider.requests()
	if len(reqs) != 2 || roles(reqs[1]) != "system,user,assistant,user" {
		t.Fatalf("second turn roles=%s (n=%d)", roles(reqs[len(reqs)-1]), len(reqs))
	}
	w := reqs[0]
	if w.Path != "/zen/go/v1/chat/completions" || w.UserAgent != "eino-channels/test" || w.Auth != "Bearer SENTINEL_API_KEY" || !strings.HasPrefix(w.Session, "ecs-") || w.Body["model"] != "deepseek-v4-flash" {
		t.Fatalf("wire=%+v", w)
	}
	if _, ok := w.Body["tools"]; ok {
		t.Fatal("tools on wire")
	}
	if _, ok := w.Body["tool_choice"]; ok {
		t.Fatal("tool_choice on wire")
	}
	if messagesOf(w)[0]["content"] != config.SystemPrompt {
		t.Fatal("system prompt missing")
	}
	env.close()

	env = openReal(t, dir, provider, config.DefaultLimits())
	defer env.close()
	r3 := env.ingest(testkit.DM("D1", "U1", "1.000003", "three"))
	env.waitDelivered(env.slack, r3.Item.ID)
	reqs = provider.requests()
	if len(reqs) != 3 || roles(reqs[2]) != "system,user,assistant,user,assistant,user" {
		t.Fatalf("third turn roles=%s", roles(reqs[2]))
	}
	if reqs[2].Session != reqs[0].Session {
		t.Fatal("provider session identity changed across restart")
	}

	// Isolation: distinct platforms, channels, threads and users never share sessions.
	routes := []state.Inbound{
		testkit.Inbound("C1", "5.000001", "U1", "5.000001", "a"),
		testkit.Inbound("C1", "5.000002", "U1", "5.000002", "b"),
		testkit.Inbound("C2", "5.000003", "U1", "5.000003", "c"),
		testkit.DM("D2", "U2", "5.000004", "d"),
		discordDM("700000000000000001", "300000000000000001", "800000000000000001", "e"),
		discordThread("700000000000000002", "100000000000000001", "300000000000000001", "800000000000000002", "f"),
	}
	before := len(provider.requests())
	var ids []int64
	for _, in := range routes {
		ids = append(ids, env.ingest(in).Item.ID)
	}
	for i, id := range ids {
		d := env.slack
		if routes[i].Route.Platform == state.PlatformDiscord {
			d = env.discord
		}
		env.waitDelivered(d, id)
	}
	reqs = provider.requests()[before:]
	seen := map[string]bool{}
	for _, r := range reqs {
		if len(messagesOf(r)) != 2 {
			t.Fatalf("shared history across routes: %s", roles(r))
		}
		if seen[r.Session] {
			t.Fatalf("session header reused: %s", r.Session)
		}
		seen[r.Session] = true
	}
	if seen[reqs[0].Session] && reqs[0].Session == provider.requests()[0].Session {
		t.Fatal("route shared the DM session")
	}
	// Stored configuration carries no secret.
	all := env.logs.String()
	for _, d := range []*testkit.Deliverer{env.slack, env.discord} {
		for _, c := range d.Calls() {
			all += c.Text
		}
	}
	if strings.Contains(all, "SENTINEL_API_KEY") {
		t.Fatal("secret leaked")
	}
}

// Gate 2 (part): kill after model terminal before the host saved terminal
// state; reopen finalizes from committed text with no new inference.
func TestKillAfterTerminalBeforeDeliverySave(t *testing.T) {
	dir := t.TempDir()
	provider := newFakeOpenCode(t)
	env := openReal(t, dir, provider, config.DefaultLimits())
	r := env.ingest(testkit.DM("D1", "U1", "2.000001", "commit"))
	// Wait until the run is admitted and completed on the agent side, then
	// simulate death before the host records terminal state by closing
	// everything while the item is still admitted.
	var item state.Item
	testkit.Eventually(t, 30*time.Second, func() bool {
		it, err := env.store.GetItem(context.Background(), r.Item.ID)
		if err != nil || it.RunID == "" {
			return false
		}
		item = it
		run, err := env.bridge.Run(context.Background(), session.RunID(it.RunID))
		return err == nil && run.Terminal()
	}, "run terminal")
	// Force the host back to admitted (as if MarkTerminal never happened).
	env.close()
	st, err := state.Open(context.Background(), dir, state.Options{SkipAgentDB: true})
	if err != nil {
		t.Fatal(err)
	}
	if it, _ := st.GetItem(context.Background(), item.ID); it.State == state.StateTerminal {
		// Reset by re-running the transition path: terminal rows cannot go
		// back through the public API, so this scenario is simulated by a
		// direct SQL update in the raw store.
		if err := resetToAdmitted(t, dir, item.ID); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.Close()
	env = openReal(t, dir, provider, config.DefaultLimits())
	defer env.close()
	texts := env.waitDelivered(env.slack, r.Item.ID)
	if texts[0] != "reply:commit" {
		t.Fatalf("delivered=%q", texts)
	}
	if n := len(provider.requests()); n != 1 {
		t.Fatalf("provider requests=%d", n)
	}
}

// Gate 6: auth/quota/429/truncated SSE settle failed with one request each.
func TestProviderFailuresSettleWithoutRetryOrFallback(t *testing.T) {
	dir := t.TempDir()
	provider := newFakeOpenCode(t)
	env := openReal(t, dir, provider, config.DefaultLimits())
	defer env.close()
	for i, mode := range []string{"401", "429", "truncate"} {
		provider.push(mode)
		r := env.ingest(testkit.DM("D1", "U1", "3.00000"+string(rune('1'+i)), "m"+mode))
		texts := env.waitDelivered(env.slack, r.Item.ID)
		if texts[0] != conversation.TextFailed {
			t.Fatalf("mode %s delivered=%q", mode, texts)
		}
		if n := len(provider.requests()); n != i+1 {
			t.Fatalf("mode %s requests=%d (SDK retry?)", mode, n)
		}
	}
	all := env.logs.String()
	for _, c := range env.slack.Calls() {
		all += c.Text
	}
	if strings.Contains(all, "SENTINEL_PROVIDER_BODY") || strings.Contains(all, "SENTINEL_API_KEY") {
		t.Fatal("provider body or key leaked into diagnostics")
	}
	// The next turn works normally (no fallback provider was selected).
	r := env.ingest(testkit.DM("D1", "U1", "3.000009", "ok"))
	if texts := env.waitDelivered(env.slack, r.Item.ID); texts[0] != "reply:ok" {
		t.Fatalf("delivered=%q", texts)
	}
}

// Slow stream: preview visible before EOF, exact durable final after.
func TestSlowStreamPreviewThenExactFinal(t *testing.T) {
	dir := t.TempDir()
	provider := newFakeOpenCode(t)
	provider.mu.Lock()
	provider.delay = 3 * time.Second
	provider.mu.Unlock()
	env := openReal(t, dir, provider, config.DefaultLimits())
	defer env.close()
	r := env.ingest(testkit.DM("D1", "U1", "4.000001", "slowly"))
	testkit.Eventually(t, 10*time.Second, func() bool {
		txt := env.slack.Text("m1")
		return txt != "" && txt != "Thinking…" && !strings.HasPrefix(txt, "reply:slowly")
	}, "preview shows a prefix before completion")
	texts := env.waitDelivered(env.slack, r.Item.ID)
	if texts[0] != "reply:slowly" {
		t.Fatalf("final=%q", texts)
	}
	_ = agentbridge.ProviderSessionID
}
