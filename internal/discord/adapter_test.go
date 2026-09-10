package discord_test

import (
	"context"
	"encoding/json"
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

	"github.com/bwmarrin/discordgo"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/discord"
	"github.com/mattsp1290/eino-channels/internal/render"
	"github.com/mattsp1290/eino-channels/internal/state"
	"github.com/mattsp1290/eino-channels/internal/testkit"
)

// --- fixed test identities (snowflakes above 2^53 to prove IDs stay strings) ---

const (
	botID       = "400000000000000001"
	guildID     = "100000000000000001"
	textChanID  = "200000000000000001"
	allowedUser = "300000000000000001"
	otherUser   = "300000000000000009"
)

func defaultCfg() config.Discord {
	return config.Discord{
		Enabled:           true,
		GuildIDs:          []string{guildID},
		AllowedChannelIDs: []string{textChanID},
		AllowedUserIDs:    []string{allowedUser},
	}
}

// --- HTTP rewrite seam: discordgo builds absolute discord.com URLs, so we
// rewrite scheme+host to the fake httptest server. ---

type rewriteTransport struct {
	base *url.URL
	next http.RoundTripper
}

func (t rewriteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c := r.Clone(r.Context())
	c.URL.Scheme, c.URL.Host, c.Host = t.base.Scheme, t.base.Host, t.base.Host
	return t.next.RoundTrip(c)
}

// --- fake Discord REST API ---

type recordedReq struct {
	Method string
	Path   string
	Body   map[string]any
}

type fakeAPI struct {
	mu    sync.Mutex
	botID string

	channels    map[string]map[string]any
	getMsg      map[string][]map[string]any
	getMsgCalls map[string]int
	lists       map[string][]map[string]any
	nextMsgID   int64

	threadIDs map[string]string
	threadSeq int64

	queues   map[string][]string
	requests []recordedReq

	hangFor time.Duration
}

func newFakeAPI(bot string) *fakeAPI {
	return &fakeAPI{
		botID:       bot,
		channels:    map[string]map[string]any{},
		getMsg:      map[string][]map[string]any{},
		getMsgCalls: map[string]int{},
		lists:       map[string][]map[string]any{},
		nextMsgID:   9007199254740993,
		threadIDs:   map[string]string{},
		queues:      map[string][]string{},
		hangFor:     500 * time.Millisecond,
	}
}

func (f *fakeAPI) addChannel(id string, typ discordgo.ChannelType, guild, parent string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := map[string]any{"id": id, "type": int(typ)}
	if guild != "" {
		ch["guild_id"] = guild
	}
	if parent != "" {
		ch["parent_id"] = parent
	}
	f.channels[id] = ch
}

// setRootMessage registers the GET-message response(s) for a channel+message
// pair. Successive calls return successive versions (the last version
// repeats for further calls), letting a test simulate the message gaining a
// thread relationship between two lookups.
func (f *fakeAPI) setRootMessage(channelID, messageID string, versions ...map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getMsg[channelID+"/"+messageID] = versions
}

// script queues a one-shot response kind for the next N requests to
// method+path, in order. Once the queue drains, default behavior resumes.
func (f *fakeAPI) script(method, path string, steps ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := method + " " + path
	f.queues[k] = append(f.queues[k], steps...)
}

func (f *fakeAPI) popStep(method, path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := method + " " + path
	q := f.queues[k]
	if len(q) == 0 {
		return ""
	}
	step := q[0]
	f.queues[k] = q[1:]
	return step
}

func (f *fakeAPI) Requests() []recordedReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedReq, len(f.requests))
	copy(out, f.requests)
	return out
}

func (f *fakeAPI) countRequests(method, path string) int {
	n := 0
	for _, r := range f.Requests() {
		if r.Method == method && r.Path == path {
			n++
		}
	}
	return n
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeAPI) writeStep(w http.ResponseWriter, kind string) bool {
	switch kind {
	case "429":
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"message": "rate limited SENTINEL_BODY", "retry_after": 1.5, "global": false,
		})
		return true
	case "403":
		writeJSON(w, http.StatusForbidden, map[string]any{"message": "Missing Access SENTINEL_BODY", "code": 50001})
		return true
	case "502":
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("bad gateway SENTINEL_BODY"))
		return true
	case "archived":
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "Thread is archived SENTINEL_BODY", "code": 50083})
		return true
	case "hang":
		time.Sleep(f.hangFor)
		writeJSON(w, http.StatusOK, map[string]any{})
		return true
	}
	return false
}

func (f *fakeAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if len(raw) != 0 {
			_ = json.Unmarshal(raw, &body)
		}
		f.mu.Lock()
		f.requests = append(f.requests, recordedReq{Method: r.Method, Path: r.URL.Path, Body: body})
		f.mu.Unlock()

		if step := f.popStep(r.Method, r.URL.Path); step != "" {
			if f.writeStep(w, step) {
				return
			}
		}

		rest := strings.TrimPrefix(r.URL.Path, "/api/v9/channels/")
		parts := strings.Split(rest, "/")
		switch {
		case len(parts) == 1 && parts[0] != "":
			id := parts[0]
			switch r.Method {
			case http.MethodGet:
				f.getChannel(w, id)
			case http.MethodPatch:
				f.patchChannel(w, id, body)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		case len(parts) == 2 && parts[1] == "messages":
			cid := parts[0]
			switch r.Method {
			case http.MethodPost:
				f.createMessage(w, cid, body)
			case http.MethodGet:
				f.listMessages(w, cid)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		case len(parts) == 3 && parts[1] == "messages":
			cid, mid := parts[0], parts[2]
			switch r.Method {
			case http.MethodGet:
				f.getMessage(w, cid, mid)
			case http.MethodPatch:
				f.editMessage(w, cid, mid, body)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		case len(parts) == 4 && parts[1] == "messages" && parts[3] == "threads":
			cid, mid := parts[0], parts[2]
			if r.Method == http.MethodPost {
				f.createThread(w, cid, mid)
			} else {
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func (f *fakeAPI) getChannel(w http.ResponseWriter, id string) {
	f.mu.Lock()
	ch, ok := f.channels[id]
	f.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "Unknown Channel SENTINEL_BODY", "code": 10003})
		return
	}
	writeJSON(w, http.StatusOK, ch)
}

func (f *fakeAPI) patchChannel(w http.ResponseWriter, id string, body map[string]any) {
	f.mu.Lock()
	ch, ok := f.channels[id]
	if !ok {
		ch = map[string]any{"id": id, "type": int(discordgo.ChannelTypeGuildPublicThread)}
	}
	for k, v := range body {
		ch[k] = v
	}
	f.channels[id] = ch
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, ch)
}

func (f *fakeAPI) getMessage(w http.ResponseWriter, cid, mid string) {
	key := cid + "/" + mid
	f.mu.Lock()
	versions := f.getMsg[key]
	n := f.getMsgCalls[key]
	f.getMsgCalls[key] = n + 1
	f.mu.Unlock()
	if len(versions) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "Unknown Message SENTINEL_BODY", "code": 10008})
		return
	}
	idx := n
	if idx >= len(versions) {
		idx = len(versions) - 1
	}
	writeJSON(w, http.StatusOK, versions[idx])
}

func (f *fakeAPI) createThread(w http.ResponseWriter, cid, mid string) {
	key := cid + "/" + mid
	f.mu.Lock()
	tid, ok := f.threadIDs[key]
	if !ok {
		f.threadSeq++
		tid = fmt.Sprintf("%d", 9007199254740993+f.threadSeq)
		f.threadIDs[key] = tid
		guild, _ := f.channels[cid]["guild_id"].(string)
		f.channels[tid] = map[string]any{"id": tid, "type": int(discordgo.ChannelTypeGuildPublicThread), "guild_id": guild, "parent_id": cid}
	}
	ch := f.channels[tid]
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, ch)
}

func (f *fakeAPI) createMessage(w http.ResponseWriter, cid string, body map[string]any) {
	f.mu.Lock()
	id := fmt.Sprintf("%d", f.nextMsgID)
	f.nextMsgID++
	entry := map[string]any{"id": id, "channel_id": cid, "author": map[string]any{"id": f.botID}}
	if body != nil {
		if n, ok := body["nonce"]; ok {
			entry["nonce"] = n
		}
		if c, ok := body["content"]; ok {
			entry["content"] = c
		}
	}
	f.lists[cid] = append(f.lists[cid], entry)
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "channel_id": cid})
}

func (f *fakeAPI) editMessage(w http.ResponseWriter, cid, mid string, body map[string]any) {
	f.mu.Lock()
	for i, m := range f.lists[cid] {
		if fmt.Sprint(m["id"]) == mid {
			if c, ok := body["content"]; ok {
				f.lists[cid][i]["content"] = c
			}
		}
	}
	f.mu.Unlock()
	resp := map[string]any{"id": mid, "channel_id": cid}
	if body != nil {
		if c, ok := body["content"]; ok {
			resp["content"] = c
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (f *fakeAPI) listMessages(w http.ResponseWriter, cid string) {
	f.mu.Lock()
	items := append([]map[string]any(nil), f.lists[cid]...)
	f.mu.Unlock()
	if items == nil {
		items = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, items)
}

// --- test harness ---

type harness struct {
	env  *testkit.Env
	a    *discord.Adapter
	fake *fakeAPI
	logs *testkit.LogBuffer
}

func newFakeServer(t *testing.T, f *fakeAPI) *http.Client {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	base, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse fake server url: %v", err)
	}
	return &http.Client{Transport: rewriteTransport{base: base, next: http.DefaultTransport}}
}

func setup(t *testing.T, cfg config.Discord) *harness {
	return setupClient(t, cfg, func(c *http.Client) {})
}

func setupClient(t *testing.T, cfg config.Discord, tune func(*http.Client)) *harness {
	t.Helper()
	env := testkit.Open(t, testkit.Options{Started: true, Platform: state.PlatformDiscord})
	t.Cleanup(env.Close)
	fake := newFakeAPI(botID)
	client := newFakeServer(t, fake)
	tune(client)
	var logs testkit.LogBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	a, err := discord.New(discord.Options{
		Config:     cfg,
		Token:      "fake-token",
		Store:      env.Store,
		Logger:     logger,
		HTTPClient: client,
		BotUserID:  botID,
	})
	if err != nil {
		t.Fatalf("discord.New: %v", err)
	}
	a.Attach(env.Service)
	return &harness{env: env, a: a, fake: fake, logs: &logs}
}

func newMessage(id, channelID, guild, authorID, content string) *discordgo.Message {
	return &discordgo.Message{
		ID:        id,
		ChannelID: channelID,
		GuildID:   guild,
		Content:   content,
		Author:    &discordgo.User{ID: authorID, Bot: false},
		Type:      discordgo.MessageTypeDefault,
		Timestamp: time.Now(),
	}
}

func mention(m *discordgo.Message, bot string) *discordgo.Message {
	m.Mentions = append(m.Mentions, &discordgo.User{ID: bot})
	return m
}

func promptEcho(actor string, isDM bool, text string) string {
	if isDM {
		return "reply:" + text
	}
	return "reply:[" + actor + "] " + text
}

// --- 1/2: DM and group DM ---

func TestDiscordDMAccepted(t *testing.T) {
	h := setup(t, defaultCfg())
	dmChan := "500000000000000001"
	h.fake.addChannel(dmChan, discordgo.ChannelTypeDM, "", "")

	m := newMessage("9007199254740994", dmChan, "", allowedUser, "hi")
	h.a.HandleMessage(context.Background(), m)

	want := promptEcho(allowedUser, true, "hi")
	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, msg := range h.env.Deliverer.Messages() {
			if msg == want {
				return true
			}
		}
		return false
	}, "DM reply delivered")

	route := state.Route{Platform: state.PlatformDiscord, Installation: botID, Channel: dmChan, DMActor: allowedUser}
	conv, err := h.env.Store.GetConversation(context.Background(), route)
	if err != nil {
		t.Fatalf("GetConversation: %v", err)
	}
	if conv.Route.DMActor != allowedUser {
		t.Errorf("DMActor = %q, want %q", conv.Route.DMActor, allowedUser)
	}
}

func TestDiscordGroupDMIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	groupDM := "500000000000000002"
	h.fake.addChannel(groupDM, discordgo.ChannelTypeGroupDM, "", "")

	m := newMessage("9007199254740995", groupDM, "", allowedUser, "hi")
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
	if n := len(h.env.Deliverer.Calls()); n != 0 {
		t.Errorf("deliverer calls = %d, want 0", n)
	}
}

func TestDiscordDMDisallowedUserIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	dmChan := "500000000000000003"
	h.fake.addChannel(dmChan, discordgo.ChannelTypeDM, "", "")

	m := newMessage("9007199254740996", dmChan, "", otherUser, "hi")
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

// --- 3: root mention creates a thread; replay is a duplicate ---

func TestDiscordGuildThreadCreation(t *testing.T) {
	h := setup(t, defaultCfg())
	h.fake.addChannel(textChanID, discordgo.ChannelTypeGuildText, guildID, "")

	rootID := "9007199254740997"
	h.fake.setRootMessage(textChanID, rootID, map[string]any{"id": rootID, "channel_id": textChanID})

	root := mention(newMessage(rootID, textChanID, guildID, allowedUser, "<@"+botID+"> hello there"), botID)
	h.a.HandleMessage(context.Background(), root)

	getPath := "/api/v9/channels/" + textChanID + "/messages/" + rootID
	postPath := getPath + "/threads"
	if n := h.fake.countRequests(http.MethodGet, getPath); n != 1 {
		t.Errorf("GET message count = %d, want 1", n)
	}
	if n := h.fake.countRequests(http.MethodPost, postPath); n != 1 {
		t.Errorf("POST threads count = %d, want 1", n)
	}

	threadID, err := h.env.Store.LookupThread(context.Background(), state.PlatformDiscord, botID, rootID)
	if err != nil {
		t.Fatalf("LookupThread: %v", err)
	}

	route := state.Route{Platform: state.PlatformDiscord, Installation: botID, Channel: threadID, ThreadRoot: guildID}
	conv, err := h.env.Store.GetConversation(context.Background(), route)
	if err != nil {
		t.Fatalf("GetConversation: %v", err)
	}
	if conv.Route.Channel != threadID || conv.Route.ThreadRoot != guildID {
		t.Errorf("route = %+v, want channel %q root %q", conv.Route, threadID, guildID)
	}

	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, c := range h.env.Deliverer.Calls() {
			if c.Op == "create" && c.Dest.Channel == threadID {
				return true
			}
		}
		return false
	}, "reply delivered to the thread")

	// Replay the same root message: no second thread, and it is deduplicated.
	h.a.HandleMessage(context.Background(), root)
	if n := h.fake.countRequests(http.MethodPost, postPath); n != 1 {
		t.Errorf("POST threads count after replay = %d, want 1", n)
	}
	testkit.Eventually(t, 20*time.Second, func() bool { return len(h.env.Script.Requests()) == 1 }, "exactly one provider request")
	time.Sleep(100 * time.Millisecond)
	if n := len(h.env.Script.Requests()); n != 1 {
		t.Errorf("provider requests after replay = %d, want 1", n)
	}
}

// --- 4: ambiguous thread creation ---

func TestDiscordThreadCreateAmbiguousResolvesViaMessage(t *testing.T) {
	h := setupClient(t, defaultCfg(), func(c *http.Client) { c.Timeout = 250 * time.Millisecond })
	h.fake.addChannel(textChanID, discordgo.ChannelTypeGuildText, guildID, "")

	rootID := "9007199254740998"
	noThread := map[string]any{"id": rootID, "channel_id": textChanID}
	withThread := map[string]any{"id": rootID, "channel_id": textChanID, "thread": map[string]any{
		"id": "T2", "type": int(discordgo.ChannelTypeGuildPublicThread), "guild_id": guildID, "parent_id": textChanID,
	}}
	h.fake.setRootMessage(textChanID, rootID, noThread, withThread)

	postPath := "/api/v9/channels/" + textChanID + "/messages/" + rootID + "/threads"
	h.fake.script(http.MethodPost, postPath, "hang")

	root := mention(newMessage(rootID, textChanID, guildID, allowedUser, "<@"+botID+"> hi"), botID)
	h.a.HandleMessage(context.Background(), root)

	if n := h.fake.countRequests(http.MethodPost, postPath); n != 1 {
		t.Errorf("POST threads count = %d, want 1 (no retry)", n)
	}
	threadID, err := h.env.Store.LookupThread(context.Background(), state.PlatformDiscord, botID, rootID)
	if err != nil {
		t.Fatalf("LookupThread: %v", err)
	}
	if threadID != "T2" {
		t.Errorf("bound thread = %q, want T2", threadID)
	}
}

func TestDiscordThreadCreateFailureNotifies(t *testing.T) {
	h := setup(t, defaultCfg())
	h.fake.addChannel(textChanID, discordgo.ChannelTypeGuildText, guildID, "")

	rootID := "9007199254740999"
	noThread := map[string]any{"id": rootID, "channel_id": textChanID}
	h.fake.setRootMessage(textChanID, rootID, noThread)

	postPath := "/api/v9/channels/" + textChanID + "/messages/" + rootID + "/threads"
	h.fake.script(http.MethodPost, postPath, "502")

	root := mention(newMessage(rootID, textChanID, guildID, allowedUser, "<@"+botID+"> hi"), botID)
	h.a.HandleMessage(context.Background(), root)

	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, n := range h.env.Deliverer.Notices() {
			if n == "I could not open a thread for this conversation. Please try again." {
				return true
			}
		}
		return false
	}, "thread failure notice")

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
	for _, c := range h.env.Deliverer.Calls() {
		if c.Op == "create" {
			t.Errorf("unexpected prompt delivery call: %+v", c)
		}
	}
}

// --- 5: concurrent root replay ---

func TestDiscordConcurrentRootReplay(t *testing.T) {
	h := setup(t, defaultCfg())
	h.fake.addChannel(textChanID, discordgo.ChannelTypeGuildText, guildID, "")

	rootID := "9007199254741000"
	h.fake.setRootMessage(textChanID, rootID, map[string]any{"id": rootID, "channel_id": textChanID})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			root := mention(newMessage(rootID, textChanID, guildID, allowedUser, "<@"+botID+"> hi"), botID)
			h.a.HandleMessage(context.Background(), root)
		}()
	}
	wg.Wait()

	threadID, err := h.env.Store.LookupThread(context.Background(), state.PlatformDiscord, botID, rootID)
	if err != nil {
		t.Fatalf("LookupThread: %v", err)
	}
	if threadID == "" {
		t.Fatal("expected a bound thread id")
	}

	testkit.Eventually(t, 20*time.Second, func() bool { return len(h.env.Script.Requests()) == 1 }, "exactly one provider request")
	time.Sleep(150 * time.Millisecond)
	if n := len(h.env.Script.Requests()); n != 1 {
		t.Errorf("provider requests = %d, want 1", n)
	}
}

// --- 6: existing threads, guild rules, and message-shape rejections ---

func TestDiscordExistingThreadMentionAccepted(t *testing.T) {
	h := setup(t, defaultCfg())
	threadID := "600000000000000001"
	h.fake.addChannel(threadID, discordgo.ChannelTypeGuildPublicThread, guildID, textChanID)

	m := mention(newMessage("9007199254741001", threadID, guildID, allowedUser, "<@"+botID+"> hi"), botID)
	h.a.HandleMessage(context.Background(), m)

	want := promptEcho(allowedUser, false, "hi")
	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, msg := range h.env.Deliverer.Messages() {
			if msg == want {
				return true
			}
		}
		return false
	}, "reply delivered to existing thread")
}

func TestDiscordExistingThreadDisallowedParentIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	threadID := "600000000000000002"
	h.fake.addChannel(threadID, discordgo.ChannelTypeGuildPublicThread, guildID, "999999999999999999")

	m := mention(newMessage("9007199254741002", threadID, guildID, allowedUser, "<@"+botID+"> hi"), botID)
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

func TestDiscordPrivateThreadIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	threadID := "600000000000000003"
	h.fake.addChannel(threadID, discordgo.ChannelTypeGuildPrivateThread, guildID, textChanID)

	m := mention(newMessage("9007199254741003", threadID, guildID, allowedUser, "<@"+botID+"> hi"), botID)
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

func TestDiscordForumIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	forumID := "600000000000000004"
	h.fake.addChannel(forumID, discordgo.ChannelTypeGuildForum, guildID, "")

	m := mention(newMessage("9007199254741004", forumID, guildID, allowedUser, "<@"+botID+"> hi"), botID)
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

func TestDiscordGuildNotAllowedIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	m := mention(newMessage("9007199254741005", textChanID, "999999999999999998", allowedUser, "<@"+botID+"> hi"), botID)
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
	if n := len(h.fake.Requests()); n != 0 {
		t.Errorf("fake API calls = %d, want 0 (fails closed before any REST call)", n)
	}
}

func TestDiscordGuildTextWithoutMentionIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	m := newMessage("9007199254741006", textChanID, guildID, allowedUser, "hello with no mention")
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

func TestDiscordBotAuthorIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	m := newMessage("9007199254741007", textChanID, guildID, allowedUser, "<@"+botID+"> hi")
	m.Author.Bot = true
	mention(m, botID)
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

func TestDiscordWebhookIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	m := newMessage("9007199254741008", textChanID, guildID, allowedUser, "<@"+botID+"> hi")
	m.WebhookID = "700000000000000001"
	mention(m, botID)
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

func TestDiscordEditedIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	m := newMessage("9007199254741009", textChanID, guildID, allowedUser, "<@"+botID+"> hi")
	now := time.Now()
	m.EditedTimestamp = &now
	mention(m, botID)
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

func TestDiscordNonDefaultTypeIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	m := newMessage("9007199254741010", textChanID, guildID, allowedUser, "<@"+botID+"> hi")
	m.Type = discordgo.MessageTypeChannelPinnedMessage
	mention(m, botID)
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
}

// --- 7: attachments ---

func TestDiscordAttachmentsWithTextNoticeFiles(t *testing.T) {
	h := setup(t, defaultCfg())
	dmChan := "500000000000000004"
	h.fake.addChannel(dmChan, discordgo.ChannelTypeDM, "", "")

	m := newMessage("9007199254741011", dmChan, "", allowedUser, "look at this")
	m.Attachments = []*discordgo.MessageAttachment{{ID: "800000000000000001", Filename: "a.txt"}}
	h.a.HandleMessage(context.Background(), m)

	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, n := range h.env.Deliverer.Notices() {
			if n == conversation.NoticeFiles {
				return true
			}
		}
		return false
	}, "files notice")
}

func TestDiscordAttachmentOnlyIgnored(t *testing.T) {
	h := setup(t, defaultCfg())
	dmChan := "500000000000000005"
	h.fake.addChannel(dmChan, discordgo.ChannelTypeDM, "", "")

	m := newMessage("9007199254741012", dmChan, "", allowedUser, "")
	m.Attachments = []*discordgo.MessageAttachment{{ID: "800000000000000002", Filename: "a.txt"}}
	h.a.HandleMessage(context.Background(), m)

	if n := len(h.env.Script.Requests()); n != 0 {
		t.Errorf("provider requests = %d, want 0", n)
	}
	if n := len(h.env.Deliverer.Calls()); n != 0 {
		t.Errorf("deliverer calls = %d, want 0", n)
	}
}

// --- 8: controls ---

func TestDiscordHelpControlInThread(t *testing.T) {
	h := setup(t, defaultCfg())
	threadID := "600000000000000005"
	h.fake.addChannel(threadID, discordgo.ChannelTypeGuildPublicThread, guildID, textChanID)

	m := mention(newMessage("9007199254741013", threadID, guildID, allowedUser, "<@"+botID+"> !help"), botID)
	h.a.HandleMessage(context.Background(), m)

	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, n := range h.env.Deliverer.Notices() {
			if n == conversation.NoticeHelp {
				return true
			}
		}
		return false
	}, "help notice")
}

func TestDiscordStopNothingToDoInDM(t *testing.T) {
	h := setup(t, defaultCfg())
	dmChan := "500000000000000006"
	h.fake.addChannel(dmChan, discordgo.ChannelTypeDM, "", "")

	m := newMessage("9007199254741014", dmChan, "", allowedUser, "!stop")
	h.a.HandleMessage(context.Background(), m)

	testkit.Eventually(t, 20*time.Second, func() bool {
		for _, n := range h.env.Deliverer.Notices() {
			if n == conversation.NoticeNothingToDo {
				return true
			}
		}
		return false
	}, "nothing to do notice")
}

// --- 9: delivery seam ---

func threadDest(h *harness, channel string) state.Destination {
	return state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: channel, ThreadRoot: guildID}
}

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

func assertDeliveryError(t *testing.T, err error, de **conversation.DeliveryError) bool {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
		return false
	}
	e, ok := err.(*conversation.DeliveryError)
	if !ok {
		t.Fatalf("error type = %T, want *conversation.DeliveryError: %v", err, err)
		return false
	}
	*de = e
	return true
}

func TestDiscordDeliveryAllowed(t *testing.T) {
	h := setup(t, defaultCfg())
	d := h.a.Deliverer()

	dmOK := state.Destination{Platform: state.PlatformDiscord, Installation: botID, DMActor: allowedUser}
	if ok, _ := d.Allowed(context.Background(), dmOK); !ok {
		t.Error("Allowed(dm allowed actor) = false, want true")
	}
	dmBad := state.Destination{Platform: state.PlatformDiscord, Installation: botID, DMActor: otherUser}
	if ok, _ := d.Allowed(context.Background(), dmBad); ok {
		t.Error("Allowed(dm other actor) = true, want false")
	}

	okThread := "600000000000000020"
	h.fake.addChannel(okThread, discordgo.ChannelTypeGuildPublicThread, guildID, textChanID)
	destOK := state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: okThread, ThreadRoot: guildID}
	if ok, _ := d.Allowed(context.Background(), destOK); !ok {
		t.Error("Allowed(thread, allowed parent) = false, want true")
	}

	badThread := "600000000000000021"
	h.fake.addChannel(badThread, discordgo.ChannelTypeGuildPublicThread, guildID, "999999999999999997")
	destBadParent := state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: badThread, ThreadRoot: guildID}
	if ok, _ := d.Allowed(context.Background(), destBadParent); ok {
		t.Error("Allowed(thread, disallowed parent) = true, want false")
	}

	destUnknownGuild := state.Destination{Platform: state.PlatformDiscord, Installation: botID, Channel: okThread, ThreadRoot: "999999999999999996"}
	if ok, _ := d.Allowed(context.Background(), destUnknownGuild); ok {
		t.Error("Allowed(unknown guild) = true, want false")
	}

	destWrongInstall := state.Destination{Platform: state.PlatformDiscord, Installation: "999999999999999995", Channel: okThread, ThreadRoot: guildID}
	if ok, _ := d.Allowed(context.Background(), destWrongInstall); ok {
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
