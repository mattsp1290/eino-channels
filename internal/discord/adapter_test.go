package discord_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/discord"
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
