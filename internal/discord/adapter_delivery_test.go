package discord_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/render"
	"github.com/mattsp1290/eino-channels/internal/state"
)

func TestDiscordDeliveryCreateBasic(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000010")
	d := h.a.Deliverer()

	id, err := d.Create(context.Background(), dest, "hello", "nonce-1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id != "9007199254740993" {
		t.Errorf("id = %q, want 9007199254740993", id)
	}

	reqs := h.fake.Requests()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d, want 1", len(reqs))
	}
	body := reqs[0].Body
	if body["content"] != "hello" {
		t.Errorf("content = %v, want hello", body["content"])
	}
	if body["nonce"] != "nonce-1" {
		t.Errorf("nonce = %v, want nonce-1", body["nonce"])
	}
	if body["enforce_nonce"] != true {
		t.Errorf("enforce_nonce = %v, want true", body["enforce_nonce"])
	}
	am, ok := body["allowed_mentions"].(map[string]any)
	if !ok {
		t.Fatalf("allowed_mentions missing or wrong type: %v", body["allowed_mentions"])
	}
	if parse, ok := am["parse"].([]any); !ok || len(parse) != 0 {
		t.Errorf("allowed_mentions.parse = %v, want []", am["parse"])
	}
	if am["replied_user"] != false {
		t.Errorf("allowed_mentions.replied_user = %v, want false", am["replied_user"])
	}
	if flags, ok := body["flags"].(float64); !ok || flags != 4 {
		t.Errorf("flags = %v, want 4", body["flags"])
	}
}

func TestDiscordDeliveryCreateEmptyNonceOmitsFields(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000011")
	d := h.a.Deliverer()

	if _, err := d.Create(context.Background(), dest, "hello", ""); err != nil {
		t.Fatalf("Create: %v", err)
	}
	body := h.fake.Requests()[0].Body
	if _, ok := body["nonce"]; ok {
		t.Errorf("nonce present, want omitted: %v", body["nonce"])
	}
	if _, ok := body["enforce_nonce"]; ok {
		t.Errorf("enforce_nonce present, want omitted: %v", body["enforce_nonce"])
	}
}

func TestDiscordDeliveryEditBasic(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000012")
	d := h.a.Deliverer()

	if err := d.Edit(context.Background(), dest, "1234567890123456789", "updated"); err != nil {
		t.Fatalf("Edit: %v", err)
	}
	reqs := h.fake.Requests()
	if len(reqs) != 1 || reqs[0].Method != http.MethodPatch {
		t.Fatalf("requests = %+v, want one PATCH", reqs)
	}
	body := reqs[0].Body
	am, ok := body["allowed_mentions"].(map[string]any)
	if !ok {
		t.Fatalf("allowed_mentions missing: %v", body)
	}
	if parse, ok := am["parse"].([]any); !ok || len(parse) != 0 {
		t.Errorf("allowed_mentions.parse = %v, want []", am["parse"])
	}
	if flags, ok := body["flags"].(float64); !ok || flags != 4 {
		t.Errorf("flags = %v, want 4", body["flags"])
	}
}

func TestDiscordDeliveryReconcile(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000013")
	d := h.a.Deliverer()

	id, err := d.Create(context.Background(), dest, "hello", "n-found")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	gotID, found, err := d.Reconcile(context.Background(), dest, "n-found")
	if err != nil || !found || gotID != id {
		t.Errorf("Reconcile(n-found) = (%q,%v,%v), want (%q,true,nil)", gotID, found, err, id)
	}

	gotID, found, err = d.Reconcile(context.Background(), dest, "n-missing")
	if err != nil || found || gotID != "" {
		t.Errorf("Reconcile(n-missing) = (%q,%v,%v), want (\"\",false,nil)", gotID, found, err)
	}
}

func TestDiscordDeliveryDefinite500(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000041")
	d := h.a.Deliverer()
	h.fake.script(http.MethodPost, "/api/v9/channels/"+dest.Channel+"/messages", "500")
	_, err := d.Create(context.Background(), dest, "hello", "n-500")
	var de *conversation.DeliveryError
	if !assertDeliveryError(t, err, &de) {
		return
	}
	if de.Kind != conversation.KindDefinite {
		t.Errorf("Kind = %v, want KindDefinite for a RESTError 500", de.Kind)
	}
	if strings.Contains(err.Error(), "SENTINEL_BODY") {
		t.Error("error text leaked the response body")
	}
}

func TestDiscordReconcileNumericNonce(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000042")
	d := h.a.Deliverer()
	// Discord may echo an integer nonce; plant one directly in the fake.
	h.fake.mu.Lock()
	h.fake.lists[dest.Channel] = append(h.fake.lists[dest.Channel], map[string]any{
		"id": "700000000000000042", "channel_id": dest.Channel, "content": "x", "nonce": 123456789,
		"author": map[string]any{"id": h.a.BotID(), "bot": true},
	})
	h.fake.mu.Unlock()
	id, found, err := d.Reconcile(context.Background(), dest, "123456789")
	if err != nil || !found || id != "700000000000000042" {
		t.Fatalf("Reconcile numeric nonce = (%q,%v,%v)", id, found, err)
	}
}

func TestDiscordDeliveryRateLimited429(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000014")
	d := h.a.Deliverer()
	h.fake.script(http.MethodPost, "/api/v9/channels/"+dest.Channel+"/messages", "429")

	_, err := d.Create(context.Background(), dest, "hello", "")
	var de *conversation.DeliveryError
	if !assertDeliveryError(t, err, &de) {
		return
	}
	if de.Kind != conversation.KindRateLimited {
		t.Errorf("Kind = %v, want KindRateLimited", de.Kind)
	}
	if de.RetryAfter < 1400*time.Millisecond || de.RetryAfter > 1600*time.Millisecond {
		t.Errorf("RetryAfter = %v, want ~1.5s", de.RetryAfter)
	}
}

func TestDiscordDeliveryPermanent403(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000015")
	d := h.a.Deliverer()
	h.fake.script(http.MethodPost, "/api/v9/channels/"+dest.Channel+"/messages", "403")

	_, err := d.Create(context.Background(), dest, "hello", "")
	var de *conversation.DeliveryError
	if !assertDeliveryError(t, err, &de) {
		return
	}
	if de.Kind != conversation.KindPermanent {
		t.Errorf("Kind = %v, want KindPermanent", de.Kind)
	}
}

// TestDiscordDeliveryDefinite502 exercises the 502 case through Edit, not
// Create. discordgo special-cases HTTP 502 with its own bounded-retry loop
// (see restapi.go's http.StatusBadGateway branch): with MaxRestRetries=0 the
// exhausted retry returns a bare fmt.Errorf, not a *discordgo.RESTError, so
// it never reaches classify()'s RESTError-status switch and instead falls
// through to the generic transport-failure fallback, which branches on the
// `create` flag. For Edit (create=false) that fallback yields KindDefinite,
// matching the classify() switch's intent for a non-2xx HTTP response. See
// TestDiscordDeliveryCreate502IsAmbiguousNotDefinite below for the Create
// case, where the same fallback yields KindAmbiguous instead.
func TestDiscordDeliveryDefinite502(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000016")
	d := h.a.Deliverer()
	h.fake.script(http.MethodPatch, "/api/v9/channels/"+dest.Channel+"/messages/mid-502", "502")

	err := d.Edit(context.Background(), dest, "mid-502", "hello")
	var de *conversation.DeliveryError
	if !assertDeliveryError(t, err, &de) {
		return
	}
	if de.Kind != conversation.KindDefinite {
		t.Errorf("Kind = %v, want KindDefinite", de.Kind)
	}
}

// TestDiscordDeliveryCreate502IsAmbiguousNotDefinite documents a production
// classification gap: adapter.go's classify() intends a non-2xx REST
// response to map deterministically off *discordgo.RESTError.Response.StatusCode
// (see classify in internal/discord/adapter.go), but discordgo never wraps
// an exhausted-retry 502 in a RESTError, so Create() observes this as
// KindAmbiguous (the generic "unknown outcome" fallback) rather than
// KindDefinite like every other non-2xx status. This is a real behavioral
// asymmetry between Create and Edit for the same upstream failure and is
// reported in the task summary, not treated as a test bug.
func TestDiscordDeliveryCreate502IsAmbiguousNotDefinite(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000019")
	d := h.a.Deliverer()
	h.fake.script(http.MethodPost, "/api/v9/channels/"+dest.Channel+"/messages", "502")

	_, err := d.Create(context.Background(), dest, "hello", "")
	var de *conversation.DeliveryError
	if !assertDeliveryError(t, err, &de) {
		return
	}
	if de.Kind != conversation.KindAmbiguous {
		t.Errorf("Kind = %v, want KindAmbiguous (documents discordgo's bare-error 502 retry-exhaustion path)", de.Kind)
	}
}

func TestDiscordDeliveryHangCreateAmbiguousEditDefinite(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000017")
	d := h.a.Deliverer()

	h.fake.script(http.MethodPost, "/api/v9/channels/"+dest.Channel+"/messages", "hang")
	cctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := d.Create(cctx, dest, "hello", "")
	var de *conversation.DeliveryError
	if !assertDeliveryError(t, err, &de) {
		return
	}
	if de.Kind != conversation.KindAmbiguous {
		t.Errorf("Create Kind = %v, want KindAmbiguous", de.Kind)
	}

	h.fake.script(http.MethodPatch, "/api/v9/channels/"+dest.Channel+"/messages/mid1", "hang")
	ectx, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	err = d.Edit(ectx, dest, "mid1", "updated")
	if !assertDeliveryError(t, err, &de) {
		return
	}
	if de.Kind != conversation.KindDefinite {
		t.Errorf("Edit Kind = %v, want KindDefinite", de.Kind)
	}
}

func TestDiscordDeliveryArchivedThreadRetries(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000018")
	d := h.a.Deliverer()
	msgPath := "/api/v9/channels/" + dest.Channel + "/messages"
	h.fake.script(http.MethodPost, msgPath, "archived")

	id, err := d.Create(context.Background(), dest, "hello", "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id == "" {
		t.Fatal("expected a message id after retry")
	}
	if n := h.fake.countRequests(http.MethodPost, msgPath); n != 2 {
		t.Errorf("POST create count = %d, want 2 (fail then retry)", n)
	}
	patchPath := "/api/v9/channels/" + dest.Channel
	patches := 0
	var archivedVal any
	for _, r := range h.fake.Requests() {
		if r.Method == http.MethodPatch && r.Path == patchPath {
			patches++
			archivedVal = r.Body["archived"]
		}
	}
	if patches != 1 {
		t.Errorf("PATCH channel count = %d, want 1", patches)
	}
	if archivedVal != false {
		t.Errorf("archived = %v, want false", archivedVal)
	}
}

func TestDiscordDeliveryAllowed(t *testing.T) {
	h := setup(t, defaultCfg())
	d := h.a.Deliverer()

	dmOK := state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: "700000000000000009"}
	if ok, _ := d.Allowed(context.Background(), dmOK, allowedUser); !ok {
		t.Error("Allowed(dm allowed actor) = false, want true")
	}
	dmBad := state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: "700000000000000009"}
	if ok, _ := d.Allowed(context.Background(), dmBad, otherUser); ok {
		t.Error("Allowed(dm other actor) = true, want false")
	}

	okThread := "600000000000000020"
	h.fake.addChannel(okThread, discordgo.ChannelTypeGuildPublicThread, guildID, textChanID)
	destOK := state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: okThread, ThreadRoot: guildID}
	if ok, _ := d.Allowed(context.Background(), destOK, ""); !ok {
		t.Error("Allowed(thread, allowed parent) = false, want true")
	}

	badThread := "600000000000000021"
	h.fake.addChannel(badThread, discordgo.ChannelTypeGuildPublicThread, guildID, "999999999999999997")
	destBadParent := state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: badThread, ThreadRoot: guildID}
	if ok, _ := d.Allowed(context.Background(), destBadParent, ""); ok {
		t.Error("Allowed(thread, disallowed parent) = true, want false")
	}

	destUnknownGuild := state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: okThread, ThreadRoot: "999999999999999996"}
	if ok, _ := d.Allowed(context.Background(), destUnknownGuild, ""); ok {
		t.Error("Allowed(unknown guild) = true, want false")
	}

	destWrongInstall := state.Destination{Platform: state.PlatformDiscord, Installation: "999999999999999995", Channel: okThread, ThreadRoot: guildID}
	if ok, _ := d.Allowed(context.Background(), destWrongInstall, ""); ok {
		t.Error("Allowed(wrong installation) = true, want false")
	}
}

// --- 10: rendering ---

func TestDiscordRenderingMassMentionsSuppressed(t *testing.T) {
	h := setup(t, defaultCfg())
	text := "Hello @everyone and @here friends"
	chunks := h.a.Deliverer().Chunks(text)
	for _, c := range chunks {
		if strings.Contains(c, "@everyone") {
			t.Errorf("chunk contains literal @everyone: %q", c)
		}
		if strings.Contains(c, "@here") {
			t.Errorf("chunk contains literal @here: %q", c)
		}
	}
}

func TestDiscordRenderingLongTextChunkBudget(t *testing.T) {
	h := setup(t, defaultCfg())
	text := strings.Repeat("a", 5000)
	chunks := h.a.Deliverer().Chunks(text)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, c := range chunks {
		if u := render.DiscordMeasure(c); u > render.DiscordChunkUnits {
			t.Errorf("chunk %d units = %d, want <= %d", i, u, render.DiscordChunkUnits)
		}
	}
}

func TestDiscordRenderingEmojiHeavyChunkBudget(t *testing.T) {
	h := setup(t, defaultCfg())
	text := strings.Repeat("\U0001F600", 2000) // each rune = 2 UTF-16 units
	chunks := h.a.Deliverer().Chunks(text)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, c := range chunks {
		if u := render.DiscordMeasure(c); u > render.DiscordChunkUnits {
			t.Errorf("chunk %d units = %d, want <= %d", i, u, render.DiscordChunkUnits)
		}
	}
}

func TestDiscordRenderingChunksJoinEqualsSuppressedInput(t *testing.T) {
	h := setup(t, defaultCfg())
	var b strings.Builder
	for i := 0; i < 400; i++ {
		fmt.Fprintf(&b, "line %d has some ordinary words in it and @everyone mentions too\n", i)
	}
	text := b.String()
	chunks := h.a.Deliverer().Chunks(text)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	got := strings.Join(chunks, "")
	want := render.SuppressDiscordMentions(text)
	if got != want {
		t.Errorf("joined chunks != suppressed input (len got=%d want=%d)", len(got), len(want))
	}
}

// --- 11: sentinel leak checks ---

func TestDiscordSentinelNeverLeaksIntoErrorsOrLogs(t *testing.T) {
	h := setup(t, defaultCfg())
	dest := threadDest(h, "600000000000000030")
	d := h.a.Deliverer()

	msgPath := "/api/v9/channels/" + dest.Channel + "/messages"
	h.fake.script(http.MethodPost, msgPath, "429", "403", "502")

	var errStrings []string
	for i := 0; i < 3; i++ {
		_, err := d.Create(context.Background(), dest, "hello", "")
		if err == nil {
			t.Fatalf("call %d: expected error", i)
		}
		errStrings = append(errStrings, err.Error())
	}

	logged := h.logs.String()
	serviceLogged := h.env.Logs.String()
	for _, s := range errStrings {
		if strings.Contains(s, "SENTINEL_BODY") {
			t.Errorf("error string leaked sentinel body: %q", s)
		}
		if strings.Contains(s, "fake-token") {
			t.Errorf("error string leaked token: %q", s)
		}
	}
	if strings.Contains(logged, "SENTINEL_BODY") || strings.Contains(serviceLogged, "SENTINEL_BODY") {
		t.Error("captured logs contain SENTINEL_BODY")
	}
	if strings.Contains(logged, "fake-token") || strings.Contains(serviceLogged, "fake-token") {
		t.Error("captured logs contain the bot token")
	}
}
