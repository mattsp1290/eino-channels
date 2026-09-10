package app

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mattsp1290/eino-channels/internal/state"
)

func writeConfig(t *testing.T, stateDir string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	body := `{"state_dir": "` + stateDir + `", "slack": {"enabled": true, "team_id": "T012", "allowed_channel_ids": ["C012"], "allowed_user_ids": ["U012"]}, "discord": {"enabled": true, "guild_ids": ["100000000000000001"], "allowed_channel_ids": ["200000000000000001"], "allowed_user_ids": ["300000000000000001"]}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func env(values map[string]string) func(string) string {
	return func(k string) string { return values[k] }
}

func run(args []string, getenv func(string) string) (int, string, string) {
	var out, errb bytes.Buffer
	code := Main(args, &out, &errb, getenv)
	return code, out.String(), errb.String()
}

func TestVersionAndUsage(t *testing.T) {
	code, out, _ := run([]string{"version"}, env(nil))
	if code != 0 || strings.TrimSpace(out) != UserAgent {
		t.Fatalf("code=%d out=%q", code, out)
	}
	if code, _, errb := run(nil, env(nil)); code != 2 || !strings.Contains(errb, "usage") {
		t.Fatalf("code=%d err=%q", code, errb)
	}
	if code, _, _ := run([]string{"bogus"}, env(nil)); code != 2 {
		t.Fatal("unknown command accepted")
	}
}

func TestCheckConfigNeverPrintsSecrets(t *testing.T) {
	cfg := writeConfig(t, filepath.Join(t.TempDir(), "state"))
	secrets := map[string]string{"OPENCODE_API_KEY": "SENTINEL_OC", "SLACK_BOT_TOKEN": "SENTINEL_SB", "SLACK_APP_TOKEN": "SENTINEL_SA", "DISCORD_BOT_TOKEN": "SENTINEL_DB"}
	code, out, errb := run([]string{"check-config", "--config", cfg}, env(secrets))
	if code != 0 || !strings.Contains(out, "ok") || !strings.Contains(out, "credentials: present") {
		t.Fatalf("code=%d out=%q err=%q", code, out, errb)
	}
	for _, s := range secrets {
		if strings.Contains(out, s) || strings.Contains(errb, s) {
			t.Fatal("secret printed")
		}
	}
	// Missing credentials are named by variable, never by value.
	delete(secrets, "DISCORD_BOT_TOKEN")
	code, _, errb = run([]string{"check-config", "--config", cfg}, env(secrets))
	if code != 2 || !strings.Contains(errb, "DISCORD_BOT_TOKEN") {
		t.Fatalf("code=%d err=%q", code, errb)
	}
	// Invalid config file.
	bad := filepath.Join(t.TempDir(), "bad.json")
	_ = os.WriteFile(bad, []byte(`{"state_dir": "relative"}`), 0o600)
	if code, _, errb := run([]string{"check-config", "--config", bad}, env(secrets)); code != 2 || !strings.Contains(errb, "invalid configuration") {
		t.Fatalf("code=%d err=%q", code, errb)
	}
	if code, _, _ := run([]string{"check-config"}, env(secrets)); code != 2 {
		t.Fatal("missing --config accepted")
	}
}

type fakeVerifier struct{ err error }

func (f fakeVerifier) VerifyDiscord(context.Context, string, string, string) error { return f.err }

func seedDeliveries(t *testing.T, dir string) (slackID, discordID int64) {
	t.Helper()
	ctx := context.Background()
	st, err := state.Open(ctx, dir, state.Options{SkipAgentDB: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	cap := state.Capacity{MaxQueuedPerRoute: 8, MaxPendingGlobal: 256}
	mk := func(in state.Inbound, run string) int64 {
		d, err := st.Ingest(ctx, in, cap)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Transition(ctx, d.Item.ID, state.StateQueued, state.StateAdmitting, ""); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkAdmitted(ctx, d.Item.ID, run, "u", "a"); err != nil {
			t.Fatal(err)
		}
		if err := st.MarkTerminal(ctx, d.Item.ID, "completed", "completed", []state.DeliveryPlan{{ChunkIndex: 0, Text: "SENTINEL_ANSWER", Revision: 1}}); err != nil {
			t.Fatal(err)
		}
		row, err := st.DeliveryForRun(ctx, run, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.MarkAmbiguous(ctx, row.ID); err != nil {
			t.Fatal(err)
		}
		return row.ID
	}
	slackID = mk(state.Inbound{Route: state.Route{Platform: state.PlatformSlack, Installation: "T012", Channel: "C012", ThreadRoot: "1.1"}, MessageID: "1.1", Actor: "U012", ActorLabel: "U012", Kind: state.KindPrompt, Content: "SENTINEL_PROMPT", ReceivedAt: time.Now()}, "run-s")
	discordID = mk(state.Inbound{Route: state.Route{Platform: state.PlatformDiscord, Installation: "BOT", Channel: "700000000000000001", DMActor: "300000000000000001"}, MessageID: "800000000000000001", Actor: "300000000000000001", ActorLabel: "x", Kind: state.KindPrompt, Content: "SENTINEL_PROMPT", ReceivedAt: time.Now()}, "run-d")
	return slackID, discordID
}

func TestDeliveryListAndResolve(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	cfg := writeConfig(t, dir)
	secrets := env(map[string]string{"OPENCODE_API_KEY": "k", "SLACK_BOT_TOKEN": "b", "SLACK_APP_TOKEN": "a", "DISCORD_BOT_TOKEN": "SENTINEL_DTOKEN"})
	slackID, discordID := seedDeliveries(t, dir)

	code, out, errb := run([]string{"delivery", "list", "--config", cfg}, secrets)
	if code != 0 || !strings.Contains(out, "ambiguous_create") || !strings.Contains(out, "slack") || !strings.Contains(out, "discord") {
		t.Fatalf("code=%d out=%q err=%q", code, out, errb)
	}
	if strings.Contains(out, "SENTINEL") {
		t.Fatal("bodies or tokens printed")
	}
	if code, _, errb := run([]string{"delivery", "list", "--config", cfg, "--limit", "0"}, secrets); code != 2 || !strings.Contains(errb, "--limit") {
		t.Fatalf("limit validation code=%d err=%q", code, errb)
	}

	// Concurrent daemon: the lock is held.
	held, err := state.Open(context.Background(), dir, state.Options{SkipAgentDB: true})
	if err != nil {
		t.Fatal(err)
	}
	if code, _, errb := run([]string{"delivery", "list", "--config", cfg}, secrets); code != 1 || !strings.Contains(errb, "running") {
		t.Fatalf("locked code=%d err=%q", code, errb)
	}
	_ = held.Close()

	// Malformed IDs and missing flags.
	resolve := func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := deliveryResolve(append([]string{"--config", cfg}, args...), &out, &errb, secrets, fakeVerifier{})
		return code, out.String(), errb.String()
	}
	if code, _, _ := resolve("--id", "x", "--action", "resend"); code != 2 {
		t.Fatal("malformed id accepted")
	}
	if code, _, errb := resolve("--id", "999", "--action", "resend"); code != 1 || !strings.Contains(errb, "not found") {
		t.Fatalf("missing row code=%d err=%q", code, errb)
	}
	if code, _, _ := resolve("--id", itoa(slackID), "--action", "bogus"); code != 2 {
		t.Fatal("bogus action accepted")
	}
	if code, _, errb := resolve("--id", itoa(slackID), "--action", "associate-message", "--message-id", "1700000000.000200"); code != 2 || !strings.Contains(errb, "--confirm-inspected") {
		t.Fatalf("slack attestation required: code=%d err=%q", code, errb)
	}
	if code, _, _ := resolve("--id", itoa(slackID), "--action", "associate-message", "--message-id", "not-a-ts", "--confirm-inspected"); code != 2 {
		t.Fatal("bad slack ts accepted")
	}
	if code, out, errb := resolve("--id", itoa(slackID), "--action", "associate-message", "--message-id", "1700000000.000200", "--confirm-inspected"); code != 0 || !strings.Contains(out, "associated") {
		t.Fatalf("slack associate code=%d out=%q err=%q", code, out, errb)
	}
	// Repeated resolution of a now-pending row is refused.
	if code, _, errb := resolve("--id", itoa(slackID), "--action", "resend"); code != 1 || !strings.Contains(errb, "pending") {
		t.Fatalf("repeat code=%d err=%q", code, errb)
	}
	// Discord: verification failure (wrong owner/destination) is refused.
	var out2, err2 bytes.Buffer
	if code := deliveryResolve([]string{"--config", cfg, "--id", itoa(discordID), "--action", "associate-message", "--message-id", "900000000000000001"}, &out2, &err2, secrets, fakeVerifier{err: errors.New("message is not an own message in the stored destination")}); code != 1 || !strings.Contains(err2.String(), "verification failed") {
		t.Fatalf("discord verify code=%d err=%q", code, err2.String())
	}
	if code, _, _ := resolve("--id", itoa(discordID), "--action", "associate-message", "--message-id", "abc"); code != 2 {
		t.Fatal("bad snowflake accepted")
	}
	if code, out, _ := resolve("--id", itoa(discordID), "--action", "resend"); code != 0 || !strings.Contains(out, "resend") {
		t.Fatalf("resend code=%d out=%q", code, out)
	}
	st, err := state.Open(context.Background(), dir, state.Options{SkipAgentDB: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	s, _ := st.GetDelivery(context.Background(), slackID)
	d, _ := st.GetDelivery(context.Background(), discordID)
	if s.RemoteID != "1700000000.000200" || s.Status != state.DeliveryPending || s.Op != state.OpEditPending || !strings.Contains(s.Audit, "operator-attested") || s.AckedRevision == s.DesiredRevision {
		t.Fatalf("slack row=%+v", s)
	}
	if d.Status != state.DeliveryPending || d.RemoteID != "" || !strings.Contains(d.Audit, "duplicate") {
		t.Fatalf("discord row=%+v", d)
	}
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func TestServeRejectsSecondInstanceAndBadState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	held, err := state.Open(context.Background(), dir, state.Options{SkipAgentDB: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	cfg := writeConfig(t, dir)
	secrets := env(map[string]string{"OPENCODE_API_KEY": "k", "SLACK_BOT_TOKEN": "b", "SLACK_APP_TOKEN": "a", "DISCORD_BOT_TOKEN": "d"})
	code, _, errb := run([]string{"serve", "--config", cfg}, secrets)
	if code != 1 || !strings.Contains(errb, "locked") {
		t.Fatalf("code=%d err=%q", code, errb)
	}
}
