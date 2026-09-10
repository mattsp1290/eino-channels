package slack_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	einoschema "github.com/cloudwego/eino/schema"
	"github.com/mattsp1290/eino-agent/model"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/render"
	slackadapter "github.com/mattsp1290/eino-channels/internal/slack"
	"github.com/mattsp1290/eino-channels/internal/state"
	"github.com/mattsp1290/eino-channels/internal/testkit"
)

// --- fake Slack Web API ---------------------------------------------------

type fakeCall struct {
	Path string
	Form url.Values
}

type fakeResp struct {
	status     int
	ok         bool
	errCode    string
	retryAfter string
	hang       bool
	ts         string
}

type fakeSlack struct {
	mu sync.Mutex

	authTeamID string
	authUserID string
	authBotID  string
	authOK     bool

	calls       []fakeCall
	postQueue   []fakeResp
	updateQueue []fakeResp
	tsCounter   int
}

func newFakeSlack(teamID, userID, botID string, ok bool) *fakeSlack {
	return &fakeSlack{authTeamID: teamID, authUserID: userID, authBotID: botID, authOK: ok}
}

func (f *fakeSlack) queuePost(r fakeResp) {
	f.mu.Lock()
	f.postQueue = append(f.postQueue, r)
	f.mu.Unlock()
}
func (f *fakeSlack) queueUpdate(r fakeResp) {
	f.mu.Lock()
	f.updateQueue = append(f.updateQueue, r)
	f.mu.Unlock()
}

func (f *fakeSlack) Calls() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCall(nil), f.calls...)
}

func (f *fakeSlack) lastCall(path string) (fakeCall, bool) {
	calls := f.Calls()
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].Path == path {
			return calls[i], true
		}
	}
	return fakeCall{}, false
}

func (f *fakeSlack) nextTS() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tsCounter++
	return fmt.Sprintf("1700000%03d.000100", f.tsCounter)
}

func (f *fakeSlack) nextResp(isCreate bool) (fakeResp, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := &f.postQueue
	if !isCreate {
		q = &f.updateQueue
	}
	if len(*q) == 0 {
		return fakeResp{}, false
	}
	resp := (*q)[0]
	*q = (*q)[1:]
	return resp, true
}

func writeJSON(w http.ResponseWriter, status int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (f *fakeSlack) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/")
		f.mu.Lock()
		f.calls = append(f.calls, fakeCall{Path: path, Form: r.Form})
		f.mu.Unlock()

		switch path {
		case "auth.test":
			f.mu.Lock()
			ok, team, user, bot := f.authOK, f.authTeamID, f.authUserID, f.authBotID
			f.mu.Unlock()
			body := map[string]any{"ok": ok, "team_id": team, "user_id": user, "bot_id": bot}
			if !ok {
				body["error"] = "invalid_auth"
			}
			writeJSON(w, http.StatusOK, body)
		case "chat.postMessage":
			f.respond(w, r, true)
		case "chat.update":
			f.respond(w, r, false)
		default:
			writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		}
	})
}

func (f *fakeSlack) respond(w http.ResponseWriter, r *http.Request, isCreate bool) {
	resp, has := f.nextResp(isCreate)
	if !has {
		resp = fakeResp{ok: true}
	}
	if resp.hang {
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
		return
	}
	if resp.status == http.StatusTooManyRequests {
		if resp.retryAfter != "" {
			w.Header().Set("Retry-After", resp.retryAfter)
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	status := resp.status
	if status == 0 {
		status = http.StatusOK
	}
	body := map[string]any{"ok": resp.ok}
	if resp.ok {
		ts := resp.ts
		if ts == "" {
			if isCreate {
				ts = f.nextTS()
			} else {
				ts = r.Form.Get("ts")
			}
		}
		body["channel"] = r.Form.Get("channel")
		body["ts"] = ts
	} else {
		body["error"] = resp.errCode
	}
	writeJSON(w, status, body)
}

// --- harness ---------------------------------------------------------------

type harness struct {
	env     *testkit.Env
	srv     *httptest.Server
	fake    *fakeSlack
	adapter *slackadapter.Adapter
}

func newAdapter(t *testing.T, fake *fakeSlack, cfg config.Slack) (*slackadapter.Adapter, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(fake.handler())
	t.Cleanup(srv.Close)
	a, err := slackadapter.New(slackadapter.Options{
		Config:     cfg,
		BotToken:   "xoxb-fake",
		AppToken:   "xapp-fake",
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIURL:     srv.URL + "/",
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a, srv
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	env := testkit.Open(t, testkit.Options{Started: true})
	t.Cleanup(env.Close)
	fake := newFakeSlack("T1", "UBOT", "B1", true)
	a, srv := newAdapter(t, fake, config.Slack{
		Enabled:           true,
		TeamID:            "T1",
		AllowedChannelIDs: []string{"C1"},
		AllowedUserIDs:    []string{"U1"},
	})
	a.Attach(env.Service)
	if err := a.Identity(context.Background()); err != nil {
		t.Fatalf("Identity: %v", err)
	}
	return &harness{env: env, srv: srv, fake: fake, adapter: a}
}

func totalItems(t *testing.T, env *testkit.Env) int {
	t.Helper()
	total := 0
	for _, s := range []state.State{
		state.StateQueued, state.StateAdmitting, state.StateAdmitted, state.StateTerminal,
		state.StateRejected, state.StateCanceled, state.StatePending, state.StateComplete,
	} {
		n, err := env.Store.CountByState(context.Background(), s)
		if err != nil {
			t.Fatalf("CountByState(%s): %v", s, err)
		}
		total += n
	}
	return total
}

func lastUserContent(req model.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == einoschema.User {
			return req.Messages[i].Content
		}
	}
	return ""
}

// --- event builders ----------------------------------------------------------

var envCounter int64

func nextEnvelopeID() string {
	envCounter++
	return fmt.Sprintf("env-%d", envCounter)
}

func eventsAPI(teamID string, cb *slackevents.EventsAPICallbackEvent, innerType string, inner any) socketmode.Event {
	if cb == nil {
		cb = &slackevents.EventsAPICallbackEvent{Type: "event_callback", TeamID: teamID}
	}
	return socketmode.Event{
		Type:    socketmode.EventTypeEventsAPI,
		Request: &socketmode.Request{Type: "events_api", EnvelopeID: nextEnvelopeID()},
		Data: slackevents.EventsAPIEvent{
			Type:       slackevents.CallbackEvent,
			TeamID:     teamID,
			Data:       cb,
			InnerEvent: slackevents.EventsAPIInnerEvent{Type: innerType, Data: inner},
		},
	}
}

func baseAppMention(overrides func(*slackevents.AppMentionEvent)) *slackevents.AppMentionEvent {
	e := &slackevents.AppMentionEvent{
		User:      "U1",
		Text:      "<@UBOT> hello",
		TimeStamp: "1700000000.000100",
		Channel:   "C1",
	}
	if overrides != nil {
		overrides(e)
	}
	return e
}

func mentionEvt(e *slackevents.AppMentionEvent) socketmode.Event {
	return eventsAPI("T1", nil, "app_mention", e)
}

func baseDMMessage(overrides func(*slackevents.MessageEvent)) *slackevents.MessageEvent {
	e := &slackevents.MessageEvent{
		User:        "U1",
		Text:        "hi",
		TimeStamp:   "1700000010.000100",
		Channel:     "D1",
		ChannelType: "im",
	}
	if overrides != nil {
		overrides(e)
	}
	return e
}

func messageEvt(e *slackevents.MessageEvent) socketmode.Event {
	return eventsAPI("T1", nil, "message", e)
}

// --- 1. root app_mention --------------------------------------------------

func TestAppMentionRootCreatesConversation(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.adapter.HandleEvent(ctx, mentionEvt(baseAppMention(nil)))

	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, m := range h.env.Deliverer.Messages() {
			if strings.Contains(m, "reply:[U1] hello") {
				return true
			}
		}
		return false
	}, "root mention delivered")

	route := state.Route{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1", ThreadRoot: "1700000000.000100"}
	if _, err := h.env.Store.GetConversation(ctx, route); err != nil {
		t.Errorf("GetConversation: %v", err)
	}
}

// --- 2. thread follow-up ---------------------------------------------------

func TestThreadFollowUp(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.adapter.HandleEvent(ctx, mentionEvt(baseAppMention(nil)))
	testkit.Eventually(t, 20*time.Second, func() bool {
		return len(h.env.Deliverer.Messages()) >= 1
	}, "root delivered")

	t.Run("with mention same conversation", func(t *testing.T) {
		follow := mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) {
			e.Text = "<@UBOT> again"
			e.TimeStamp = "1700000000.000200"
			e.ThreadTimeStamp = "1700000000.000100"
		}))
		h.adapter.HandleEvent(ctx, follow)

		// Wait for the follow-up's committed reply, not its placeholder.
		testkit.Eventually(t, 20*time.Second, func() bool {
			n := 0
			for _, m := range h.env.Deliverer.Messages() {
				if strings.Contains(m, "reply:") {
					n++
				}
			}
			return n >= 2 && len(h.env.Script.Requests()) >= 2
		}, "follow-up delivered")

		reqs := h.env.Script.Requests()
		userCount := 0
		for _, m := range reqs[1].Messages {
			if m.Role == einoschema.User {
				userCount++
			}
		}
		if userCount != 2 {
			t.Errorf("follow-up request user message count = %d, want 2", userCount)
		}
	})

	t.Run("without mention ignored", func(t *testing.T) {
		before := totalItems(t, h.env)
		msg := messageEvt(&slackevents.MessageEvent{
			User: "U1", Text: "no mention here", TimeStamp: "1700000000.000300",
			ThreadTimeStamp: "1700000000.000100", Channel: "C1", ChannelType: "channel",
		})
		h.adapter.HandleEvent(ctx, msg)
		if got := totalItems(t, h.env); got != before {
			t.Errorf("row count changed: before=%d after=%d, want unchanged", before, got)
		}
	})
}

// --- 3. direct messages ------------------------------------------------------

func TestDirectMessage(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	t.Run("plain text no label", func(t *testing.T) {
		h.adapter.HandleEvent(ctx, messageEvt(baseDMMessage(nil)))

		testkit.Eventually(t, 20*time.Second, func() bool {
			for _, m := range h.env.Deliverer.Messages() {
				if strings.Contains(m, "reply:hi") {
					return true
				}
			}
			return false
		}, "dm delivered")

		reqs := h.env.Script.Requests()
		if len(reqs) == 0 {
			t.Fatal("no provider requests recorded")
		}
		if got := lastUserContent(reqs[len(reqs)-1]); got != "hi" {
			t.Errorf("last user content = %q, want %q (no [U1] label)", got, "hi")
		}

		route := state.Route{Platform: state.PlatformSlack, Installation: "T1", Channel: "D1", DMActor: "U1"}
		if _, err := h.env.Store.GetConversation(ctx, route); err != nil {
			t.Errorf("GetConversation: %v", err)
		}
	})

	t.Run("mention stripped", func(t *testing.T) {
		evt := messageEvt(baseDMMessage(func(e *slackevents.MessageEvent) {
			e.Text = "<@UBOT> hi again"
			e.TimeStamp = "1700000010.000200"
		}))
		h.adapter.HandleEvent(ctx, evt)

		testkit.Eventually(t, 20*time.Second, func() bool {
			for _, m := range h.env.Deliverer.Messages() {
				if strings.Contains(m, "reply:hi again") {
					return true
				}
			}
			return false
		}, "dm with mention delivered")
	})
}

// --- 4. dedup across representations ---------------------------------------

func TestDedupAcrossRepresentations(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ts := "1700000030.000100"

	mention := eventsAPI("T1", nil, "app_mention", &slackevents.AppMentionEvent{
		User: "U1", Text: "<@UBOT> ping", TimeStamp: ts, Channel: "D1",
	})
	h.adapter.HandleEvent(ctx, mention)

	dup := eventsAPI("T1", nil, "message", &slackevents.MessageEvent{
		User: "U1", Text: "ping", TimeStamp: ts, Channel: "D1", ChannelType: "im",
	})
	h.adapter.HandleEvent(ctx, dup)

	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, m := range h.env.Deliverer.Messages() {
			if strings.Contains(m, "reply:") {
				return true
			}
		}
		return false
	}, "dedup message delivered")

	if got := len(h.env.Deliverer.Messages()); got != 1 {
		t.Errorf("Deliverer.Messages() len = %d, want 1", got)
	}
	if got := len(h.env.Script.Requests()); got != 1 {
		t.Errorf("Script.Requests() len = %d, want 1", got)
	}
}

// --- 5. filters --------------------------------------------------------------

func TestFilters(t *testing.T) {
	cases := []struct {
		name string
		evt  func() socketmode.Event
	}{
		{"bot_id set", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) { e.BotID = "B2" }))
		}},
		{"user is bot user", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) { e.User = "UBOT" }))
		}},
		{"message_changed subtype", func() socketmode.Event {
			return messageEvt(baseDMMessage(func(e *slackevents.MessageEvent) { e.SubType = "message_changed" }))
		}},
		{"bot_message subtype", func() socketmode.Event {
			return messageEvt(baseDMMessage(func(e *slackevents.MessageEvent) { e.SubType = "bot_message" }))
		}},
		{"edited app_mention", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) {
				e.Edited = &slackevents.Edited{User: "U1", TimeStamp: "1700000000.000200"}
			}))
		}},
		{"edited dm message", func() socketmode.Event {
			return messageEvt(baseDMMessage(func(e *slackevents.MessageEvent) {
				e.Message = &slack.Msg{Edited: &slack.Edited{User: "U1", Timestamp: "1700000010.000200"}}
			}))
		}},
		{"user not allowlisted", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) { e.User = "U9" }))
		}},
		{"channel not allowlisted", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) { e.Channel = "C9" }))
		}},
		{"team mismatch", func() socketmode.Event {
			return eventsAPI("T2", nil, "app_mention", baseAppMention(nil))
		}},
		{"ext shared channel", func() socketmode.Event {
			return eventsAPI("T1", &slackevents.EventsAPICallbackEvent{Type: "event_callback", TeamID: "T1", IsExtSharedChannel: true}, "app_mention", baseAppMention(nil))
		}},
		{"user_team mismatch", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) { e.UserTeam = "T2" }))
		}},
		{"source_team mismatch", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) { e.SourceTeam = "T2" }))
		}},
		{"blank text after mention strip", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) { e.Text = "<@UBOT>   " }))
		}},
		{"attachment only", func() socketmode.Event {
			return mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) {
				e.Text = "<@UBOT>"
				e.Files = []slack.File{{ID: "F1"}}
			}))
		}},
		{"non callback payload type", func() socketmode.Event {
			return socketmode.Event{
				Type:    socketmode.EventTypeEventsAPI,
				Request: &socketmode.Request{Type: "events_api", EnvelopeID: nextEnvelopeID()},
				Data:    slackevents.EventsAPIEvent{Type: slackevents.URLVerification, TeamID: "T1"},
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.adapter.HandleEvent(context.Background(), tc.evt())
			if got := totalItems(t, h.env); got != 0 {
				t.Errorf("row count = %d, want 0", got)
			}
			if got := len(h.env.Script.Requests()); got != 0 {
				t.Errorf("provider requests = %d, want 0", got)
			}
		})
	}
}

// --- 6. files with text ------------------------------------------------------

func TestFilesWithTextAcceptedWithNotice(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	evt := mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) {
		e.Text = "<@UBOT> check this out"
		e.Files = []slack.File{{ID: "F1"}}
	}))
	h.adapter.HandleEvent(ctx, evt)

	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, n := range h.env.Deliverer.Notices() {
			if n == conversation.NoticeFiles {
				return true
			}
		}
		return false
	}, "files notice delivered")
}

// --- 7. controls via slack ----------------------------------------------------

func TestControlCommandsViaSlack(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	t.Run("stop with nothing running", func(t *testing.T) {
		evt := mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) {
			e.Text = "<@UBOT> !stop"
			e.TimeStamp = "1700000040.000100"
		}))
		h.adapter.HandleEvent(ctx, evt)

		testkit.Eventually(t, 20*time.Second, func() bool {
			for _, n := range h.env.Deliverer.Notices() {
				if n == conversation.NoticeNothingToDo {
					return true
				}
			}
			return false
		}, "nothing-to-do notice delivered")
	})

	t.Run("help", func(t *testing.T) {
		evt := mentionEvt(baseAppMention(func(e *slackevents.AppMentionEvent) {
			e.Text = "<@UBOT> !help"
			e.TimeStamp = "1700000050.000100"
		}))
		h.adapter.HandleEvent(ctx, evt)

		testkit.Eventually(t, 20*time.Second, func() bool {
			for _, n := range h.env.Deliverer.Notices() {
				if n == conversation.NoticeHelp {
					return true
				}
			}
			return false
		}, "help notice delivered")
	})
}

// --- 8. delivery seam --------------------------------------------------------

func TestDeliverySeam(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	d := h.adapter.Deliverer()

	t.Run("create threaded posts expected form fields", func(t *testing.T) {
		h.fake.queuePost(fakeResp{ok: true, ts: "1700000099.000100"})
		dest := state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1", ThreadRoot: "1700000000.000100"}
		ts, err := d.Create(ctx, dest, "hello & <world>", "")
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if ts != "1700000099.000100" {
			t.Errorf("Create returned ts = %q, want %q", ts, "1700000099.000100")
		}

		call, found := h.fake.lastCall("chat.postMessage")
		if !found {
			t.Fatalf("no chat.postMessage call recorded")
		}
		form := call.Form
		if got := form.Get("thread_ts"); got != dest.ThreadRoot {
			t.Errorf("thread_ts = %q, want %q", got, dest.ThreadRoot)
		}
		if got := form.Get("parse"); got != "none" {
			t.Errorf("parse = %q, want %q", got, "none")
		}
		if got := form.Get("link_names"); got != "false" {
			t.Errorf("link_names = %q, want %q", got, "false")
		}
		if got := form.Get("unfurl_links"); got != "false" {
			t.Errorf("unfurl_links = %q, want %q", got, "false")
		}
		if got := form.Get("unfurl_media"); got != "false" {
			t.Errorf("unfurl_media = %q, want %q", got, "false")
		}
		if got := form.Get("text"); got != "hello & <world>" {
			t.Errorf("text = %q, want unescaped original %q", got, "hello & <world>")
		}
	})

	t.Run("edit updates message", func(t *testing.T) {
		dest := state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}
		if err := d.Edit(ctx, dest, "1700000000.000555", "updated text"); err != nil {
			t.Fatalf("Edit: %v", err)
		}
		call, found := h.fake.lastCall("chat.update")
		if !found {
			t.Fatalf("no chat.update call recorded")
		}
		if got := call.Form.Get("ts"); got != "1700000000.000555" {
			t.Errorf("ts = %q, want %q", got, "1700000000.000555")
		}
		if got := call.Form.Get("parse"); got != "none" {
			t.Errorf("parse = %q, want %q", got, "none")
		}
	})

	t.Run("rate limited maps to KindRateLimited", func(t *testing.T) {
		h.fake.queuePost(fakeResp{status: http.StatusTooManyRequests, retryAfter: "2"})
		dest := state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}
		_, err := d.Create(ctx, dest, "x", "")
		var de *conversation.DeliveryError
		if !errors.As(err, &de) {
			t.Fatalf("Create err = %v, want *conversation.DeliveryError", err)
		}
		if de.Kind != conversation.KindRateLimited {
			t.Errorf("Kind = %v, want KindRateLimited", de.Kind)
		}
		if de.RetryAfter != 2*time.Second {
			t.Errorf("RetryAfter = %v, want 2s", de.RetryAfter)
		}
	})

	t.Run("channel_not_found maps to KindPermanent", func(t *testing.T) {
		h.fake.queuePost(fakeResp{ok: false, errCode: "channel_not_found"})
		dest := state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}
		_, err := d.Create(ctx, dest, "x", "")
		var de *conversation.DeliveryError
		if !errors.As(err, &de) {
			t.Fatalf("Create err = %v, want *conversation.DeliveryError", err)
		}
		if de.Kind != conversation.KindPermanent {
			t.Errorf("Kind = %v, want KindPermanent", de.Kind)
		}
	})

	t.Run("unknown api error maps to KindDefinite", func(t *testing.T) {
		h.fake.queuePost(fakeResp{ok: false, errCode: "invalid_arguments"})
		dest := state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}
		_, err := d.Create(ctx, dest, "x", "")
		var de *conversation.DeliveryError
		if !errors.As(err, &de) {
			t.Fatalf("Create err = %v, want *conversation.DeliveryError", err)
		}
		if de.Kind != conversation.KindDefinite {
			t.Errorf("Kind = %v, want KindDefinite", de.Kind)
		}
	})

	t.Run("fatal_error on create is ambiguous, on edit definite", func(t *testing.T) {
		dest := state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}
		for _, code := range []string{"fatal_error", "internal_error"} {
			h.fake.queuePost(fakeResp{ok: false, errCode: code})
			_, err := d.Create(ctx, dest, "x", "")
			var de *conversation.DeliveryError
			if !errors.As(err, &de) || de.Kind != conversation.KindAmbiguous {
				t.Errorf("create %s: err = %v, want KindAmbiguous", code, err)
			}
			h.fake.queueUpdate(fakeResp{ok: false, errCode: code})
			err = d.Edit(ctx, dest, "1700000000.000100", "x")
			if !errors.As(err, &de) || de.Kind != conversation.KindDefinite {
				t.Errorf("edit %s: err = %v, want KindDefinite", code, err)
			}
		}
	})

	t.Run("hanging create maps to KindAmbiguous", func(t *testing.T) {
		h.fake.queuePost(fakeResp{hang: true})
		cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		dest := state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}
		_, err := d.Create(cctx, dest, "x", "")
		var de *conversation.DeliveryError
		if !errors.As(err, &de) {
			t.Fatalf("Create err = %v, want *conversation.DeliveryError", err)
		}
		if de.Kind != conversation.KindAmbiguous {
			t.Errorf("Kind = %v, want KindAmbiguous", de.Kind)
		}
	})

	t.Run("hanging edit maps to KindDefinite", func(t *testing.T) {
		h.fake.queueUpdate(fakeResp{hang: true})
		cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		dest := state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}
		err := d.Edit(cctx, dest, "1700000000.000999", "x")
		var de *conversation.DeliveryError
		if !errors.As(err, &de) {
			t.Fatalf("Edit err = %v, want *conversation.DeliveryError", err)
		}
		if de.Kind != conversation.KindDefinite {
			t.Errorf("Kind = %v, want KindDefinite", de.Kind)
		}
	})

	t.Run("allowed dm actor", func(t *testing.T) {
		if ok, _ := d.Allowed(context.Background(), state.Destination{Platform: state.PlatformSlack, Installation: "T1", DMActor: "U1"}); !ok {
			t.Error("expected allowed for DMActor U1")
		}
		if ok, _ := d.Allowed(context.Background(), state.Destination{Platform: state.PlatformSlack, Installation: "T1", DMActor: "U9"}); ok {
			t.Error("expected denied for DMActor U9")
		}
	})

	t.Run("allowed channel", func(t *testing.T) {
		if ok, _ := d.Allowed(context.Background(), state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}); !ok {
			t.Error("expected allowed for channel C1")
		}
		if ok, _ := d.Allowed(context.Background(), state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C9"}); ok {
			t.Error("expected denied for channel C9")
		}
	})

	t.Run("allowed wrong team", func(t *testing.T) {
		if ok, _ := d.Allowed(context.Background(), state.Destination{Platform: state.PlatformSlack, Installation: "T2", Channel: "C1"}); ok {
			t.Error("expected denied for wrong team")
		}
	})

	t.Run("allowed unaffected by socket auth health", func(t *testing.T) {
		h2 := newHarness(t)
		d2 := h2.adapter.Deliverer()
		if ok, _ := d2.Allowed(context.Background(), state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}); !ok {
			t.Fatal("expected allowed before invalid auth")
		}
		h2.adapter.HandleEvent(ctx, socketmode.Event{Type: socketmode.EventTypeInvalidAuth})
		if h2.adapter.Healthy() {
			t.Error("expected unhealthy after invalid auth event")
		}
		// Web API delivery is independent of Socket Mode health: stored
		// output must not be failed permanently by a transport blip.
		if ok, _ := d2.Allowed(context.Background(), state.Destination{Platform: state.PlatformSlack, Installation: "T1", Channel: "C1"}); !ok {
			t.Error("expected still allowed after invalid auth event")
		}
	})
}

// --- 9. rendering --------------------------------------------------------------

func TestRendering(t *testing.T) {
	h := newHarness(t)
	d := h.adapter.Deliverer()

	t.Run("escape and single chunk", func(t *testing.T) {
		got := d.Chunks("a & b <@U123> <!channel>")
		want := "a &amp; b &lt;@U123&gt; &lt;!channel&gt;"
		if len(got) != 1 || got[0] != want {
			t.Errorf("Chunks() = %#v, want [%q]", got, want)
		}
	})

	t.Run("long text respects chunk budget", func(t *testing.T) {
		base := strings.Repeat("lorem ipsum dolor sit amet ", 400)
		text := base[:10000]
		chunks := d.Chunks(text)
		if len(chunks) == 0 {
			t.Fatal("no chunks returned")
		}
		for i, c := range chunks {
			if n := utf8.RuneCountInString(c); n > render.SlackChunkChars {
				t.Errorf("chunk %d rune count = %d, exceeds budget %d", i, n, render.SlackChunkChars)
			}
		}
		if joined := strings.Join(chunks, ""); joined != render.EscapeSlack(text) {
			t.Error("joined chunks do not reproduce the escaped input")
		}
	})

	t.Run("code fence spanning boundary stays balanced", func(t *testing.T) {
		const fenceLine = "```go"
		text := "intro\n" + fenceLine + "\n" + strings.Repeat("code line filler content here\n", 400) + "```\n" + "outro\n"
		chunks := d.Chunks(text)
		if len(chunks) < 2 {
			t.Fatalf("expected the fenced block to split into multiple chunks, got %d", len(chunks))
		}
		for i, c := range chunks {
			n := 0
			for _, line := range strings.Split(c, "\n") {
				if strings.HasPrefix(strings.TrimLeft(line, " "), "```") {
					n++
				}
			}
			if n%2 != 0 {
				t.Errorf("chunk %d has an odd number of fence lines (%d), fences unbalanced", i, n)
			}
			if m := utf8.RuneCountInString(c); m > render.SlackChunkChars {
				t.Errorf("chunk %d rune count = %d, exceeds budget %d", i, m, render.SlackChunkChars)
			}
		}
	})

	t.Run("unicode preserved without fences", func(t *testing.T) {
		text := strings.Repeat("日本語テスト😀🎉👍", 300)
		chunks := d.Chunks(text)
		if joined := strings.Join(chunks, ""); joined != render.EscapeSlack(text) {
			t.Error("joined unicode chunks do not reproduce the escaped input")
		}
	})

	t.Run("preview empty returns placeholder", func(t *testing.T) {
		if got := d.Preview(""); got != render.ThinkingPlaceholder {
			t.Errorf("Preview(\"\") = %q, want %q", got, render.ThinkingPlaceholder)
		}
	})
}

// --- 10. identity --------------------------------------------------------------

func assertNoTokenLeak(t *testing.T, err error) {
	t.Helper()
	msg := err.Error()
	if strings.Contains(msg, "xoxb-fake") || strings.Contains(msg, "xapp-fake") {
		t.Errorf("error leaks fake token text: %q", msg)
	}
}

func TestIdentity(t *testing.T) {
	cfg := config.Slack{Enabled: true, TeamID: "T1", AllowedChannelIDs: []string{"C1"}, AllowedUserIDs: []string{"U1"}}

	t.Run("wrong team returns error", func(t *testing.T) {
		fake := newFakeSlack("T2", "UBOT", "B1", true)
		a, _ := newAdapter(t, fake, cfg)
		err := a.Identity(context.Background())
		if err == nil {
			t.Fatal("expected error for team mismatch")
		}
		assertNoTokenLeak(t, err)
		if a.Healthy() {
			t.Error("expected Healthy() false after failed identity")
		}
	})

	t.Run("no bot user id returns error", func(t *testing.T) {
		fake := newFakeSlack("T1", "", "B1", true)
		a, _ := newAdapter(t, fake, cfg)
		err := a.Identity(context.Background())
		if err == nil {
			t.Fatal("expected error for missing bot user id")
		}
		assertNoTokenLeak(t, err)
	})

	t.Run("success", func(t *testing.T) {
		fake := newFakeSlack("T1", "UBOT", "B1", true)
		a, _ := newAdapter(t, fake, cfg)
		if err := a.Identity(context.Background()); err != nil {
			t.Fatalf("Identity: %v", err)
		}
		if !a.Healthy() {
			t.Error("expected Healthy() true after successful identity")
		}
	})
}
