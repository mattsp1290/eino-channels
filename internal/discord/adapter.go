// Package discord is the Discord Gateway ingress and REST delivery adapter.
// DMs and explicit bot mentions in allowed guild text channels or their
// public threads are accepted; a root mention creates a public thread that
// is bound durably before any model admission.
package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/render"
	"github.com/mattsp1290/eino-channels/internal/state"
)

const (
	threadName          = "Conversation"
	threadArchiveMins   = 1440
	channelCacheEntries = 512
	channelCacheTTL     = 10 * time.Minute
	reconcileWindow     = 100
	noticeThreadFailure = "I could not open a thread for this conversation. Please try again."
)

// Options configure the adapter.
type Options struct {
	Config    config.Discord
	Token     string
	Service   *conversation.Service
	Store     *state.Store
	Logger    *slog.Logger
	UserAgent string
	// HTTPClient is a test seam for a fake REST API.
	HTTPClient *http.Client
	// BotUserID lets tests skip the Gateway ready handshake.
	BotUserID string
}

type cachedChannel struct {
	ch      *discordgo.Channel
	fetched time.Time
}

// Adapter is the Discord transport.
type Adapter struct {
	cfg      config.Discord
	users    config.Allowlist
	guilds   config.Allowlist
	channels config.Allowlist
	s        *discordgo.Session
	svc      *conversation.Service
	st       *state.Store
	log      *slog.Logger

	botID   atomic.Value // string
	ready   chan struct{}
	once    sync.Once
	mu      sync.Mutex
	cache   map[string]cachedChannel
	mention *regexp.Regexp
	healthy atomic.Bool
}

// New builds the adapter without network calls.
func New(opts Options) (*Adapter, error) {
	if opts.Store == nil || opts.Token == "" {
		return nil, errors.New("discord: service, store and token required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	s, err := discordgo.New("Bot " + opts.Token)
	if err != nil {
		return nil, fmt.Errorf("discord: session: %w", err)
	}
	s.Identify.Intents = discordgo.IntentGuilds | discordgo.IntentGuildMessages | discordgo.IntentDirectMessages
	s.ShouldRetryOnRateLimit = false
	s.MaxRestRetries = 0
	s.StateEnabled = false
	s.LogLevel = discordgo.LogError
	if opts.UserAgent != "" {
		s.UserAgent = opts.UserAgent
	}
	if opts.HTTPClient != nil {
		s.Client = opts.HTTPClient
	}
	a := &Adapter{
		cfg: opts.Config, users: config.NewAllowlist(opts.Config.AllowedUserIDs), guilds: config.NewAllowlist(opts.Config.GuildIDs), channels: config.NewAllowlist(opts.Config.AllowedChannelIDs),
		s: s, svc: opts.Service, st: opts.Store, log: opts.Logger, ready: make(chan struct{}), cache: map[string]cachedChannel{},
	}
	a.botID.Store("")
	if opts.BotUserID != "" {
		a.setBot(opts.BotUserID)
	}
	return a, nil
}

func (a *Adapter) setBot(id string) {
	a.botID.Store(id)
	a.mu.Lock()
	a.mention = regexp.MustCompile(`<@!?` + regexp.QuoteMeta(id) + `>`)
	a.mu.Unlock()
	a.healthy.Store(true)
	a.once.Do(func() { close(a.ready) })
}

// Attach binds the conversation service. It must be called before Run.
func (a *Adapter) Attach(svc *conversation.Service) { a.svc = svc }

// BotID returns the bot user ID once ready.
func (a *Adapter) BotID() string { v, _ := a.botID.Load().(string); return v }

// Healthy reports gateway readiness.
func (a *Adapter) Healthy() bool { return a.healthy.Load() }

// Run opens the Gateway, waits for readiness and serves until ctx ends.
func (a *Adapter) Run(ctx context.Context) error {
	if a.svc == nil {
		return errors.New("discord: conversation service not attached")
	}
	a.s.AddHandler(func(_ *discordgo.Session, r *discordgo.Ready) {
		if r.User != nil {
			a.setBot(r.User.ID)
		}
	})
	a.s.AddHandler(func(_ *discordgo.Session, _ *discordgo.Disconnect) {
		a.healthy.Store(false)
		a.log.Warn("discord: gateway disconnected; SDK will reconnect. Prompts sent while disconnected may be missed unless the session resumes.")
	})
	a.s.AddHandler(func(_ *discordgo.Session, _ *discordgo.Resumed) { a.healthy.Store(true) })
	a.s.AddHandler(func(_ *discordgo.Session, m *discordgo.MessageCreate) {
		if m.Message != nil {
			a.HandleMessage(ctx, m.Message)
		}
	})
	if err := a.s.Open(); err != nil {
		return fmt.Errorf("discord: gateway open failed: %w", errors.New(safeErr(err)))
	}
	select {
	case <-a.ready:
	case <-time.After(30 * time.Second):
		_ = a.s.Close()
		return errors.New("discord: gateway did not become ready")
	case <-ctx.Done():
		_ = a.s.Close()
		return nil
	}
	<-ctx.Done()
	return a.s.Close()
}

// Close closes the gateway session.
func (a *Adapter) Close() error { return a.s.Close() }

// HandleMessage validates and ingests one message. Exported for tests.
func (a *Adapter) HandleMessage(ctx context.Context, m *discordgo.Message) {
	bot := a.BotID()
	if bot == "" || m == nil || m.Author == nil || m.Author.Bot || m.Author.ID == bot || m.WebhookID != "" || m.EditedTimestamp != nil {
		return
	}
	if m.Type != discordgo.MessageTypeDefault && m.Type != discordgo.MessageTypeReply {
		return
	}
	if !a.users.Contains(m.Author.ID) {
		return
	}
	a.mu.Lock()
	mention := a.mention
	a.mu.Unlock()
	text := strings.TrimSpace(mention.ReplaceAllString(m.Content, ""))
	route := state.Route{Platform: state.PlatformDiscord, Installation: bot}
	if m.GuildID == "" {
		ch, err := a.channel(ctx, m.ChannelID)
		if err != nil || ch.Type != discordgo.ChannelTypeDM {
			return
		}
		route.Channel, route.DMActor = m.ChannelID, m.Author.ID
	} else {
		if !a.guilds.Contains(m.GuildID) || !a.mentioned(m, bot) {
			return
		}
		ch, err := a.channel(ctx, m.ChannelID)
		if err != nil {
			return // fail closed when the parent cannot be resolved
		}
		switch ch.Type {
		case discordgo.ChannelTypeGuildPublicThread:
			if !a.channels.Contains(ch.ParentID) || ch.GuildID != m.GuildID {
				return
			}
			route.Channel, route.ThreadRoot = m.ChannelID, m.GuildID
		case discordgo.ChannelTypeGuildText:
			if !a.channels.Contains(m.ChannelID) || ch.GuildID != m.GuildID {
				return
			}
			if text == "" {
				return
			}
			threadID, ok := a.threadFor(ctx, m)
			if !ok {
				a.svc.Notify(state.Destination{Platform: state.PlatformDiscord, Installation: bot, Channel: m.ChannelID, ThreadRoot: m.GuildID}, noticeThreadFailure)
				return
			}
			route.Channel, route.ThreadRoot = threadID, m.GuildID
		default:
			return
		}
	}
	if text == "" {
		return
	}
	in := state.Inbound{Route: route, MessageID: m.ID, Actor: m.Author.ID, ActorLabel: m.Author.ID, Content: text, FilesNotice: len(m.Attachments) != 0, ReceivedAt: time.Now()}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := a.svc.Ingest(pctx, in)
	if err != nil {
		a.log.Warn("discord: durable ingest failed; prompt dropped (Gateway has no ack)", "error", err.Error())
		return
	}
	if resp.Notice != "" {
		a.svc.Notify(route.Destination(), resp.Notice)
	}
}

func (a *Adapter) mentioned(m *discordgo.Message, bot string) bool {
	for _, u := range m.Mentions {
		if u != nil && u.ID == bot {
			return true
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mention != nil && a.mention.MatchString(m.Content)
}

// channel resolves channel metadata through a bounded cache.
func (a *Adapter) channel(ctx context.Context, id string) (*discordgo.Channel, error) {
	a.mu.Lock()
	if c, ok := a.cache[id]; ok && time.Since(c.fetched) < channelCacheTTL {
		a.mu.Unlock()
		return c.ch, nil
	}
	a.mu.Unlock()
	ch, err := a.s.Channel(id, discordgo.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	if len(a.cache) >= channelCacheEntries {
		for k := range a.cache {
			delete(a.cache, k)
			break
		}
	}
	a.cache[id] = cachedChannel{ch: ch, fetched: time.Now()}
	a.mu.Unlock()
	return ch, nil
}

// threadFor returns the bound thread for a root message, creating one when
// needed. Ambiguous creation is reconciled through the message's thread
// relationship before any retry; the binding is durable before return.
func (a *Adapter) threadFor(ctx context.Context, m *discordgo.Message) (string, bool) {
	bot := a.BotID()
	if id, err := a.st.LookupThread(ctx, state.PlatformDiscord, bot, m.ID); err == nil {
		return id, true
	}
	// A previous ambiguous creation may already have produced the thread.
	if msg, err := a.s.ChannelMessage(m.ChannelID, m.ID, discordgo.WithContext(ctx)); err == nil && msg != nil && msg.Thread != nil && msg.Thread.ID != "" {
		return a.bind(ctx, m.ID, msg.Thread.ID)
	}
	if m.Thread != nil && m.Thread.ID != "" {
		return a.bind(ctx, m.ID, m.Thread.ID)
	}
	ch, err := a.s.MessageThreadStartComplex(m.ChannelID, m.ID, &discordgo.ThreadStart{Name: threadName, AutoArchiveDuration: threadArchiveMins}, discordgo.WithContext(ctx))
	if err != nil || ch == nil {
		// Unknown outcome: check once more through the message relationship.
		if msg, err := a.s.ChannelMessage(m.ChannelID, m.ID, discordgo.WithContext(ctx)); err == nil && msg != nil && msg.Thread != nil {
			return a.bind(ctx, m.ID, msg.Thread.ID)
		}
		return "", false
	}
	return a.bind(ctx, m.ID, ch.ID)
}

func (a *Adapter) bind(ctx context.Context, sourceID, threadID string) (string, bool) {
	bound, err := a.st.BindThread(ctx, state.PlatformDiscord, a.BotID(), sourceID, threadID)
	if err != nil {
		return "", false
	}
	return bound, true
}

// --- delivery -----------------------------------------------------------------

// Deliverer exposes the REST delivery seam.
type Deliverer struct{ a *Adapter }

// Deliverer returns the delivery implementation.
func (a *Adapter) Deliverer() *Deliverer { return &Deliverer{a: a} }

var noMentions = map[string]any{"parse": []string{}, "replied_user": false}

type createdMessage struct {
	ID string `json:"id"`
}

// Create posts a message with a nonce and enforce_nonce on the wire, no
// mentions and suppressed embeds.
func (d *Deliverer) Create(ctx context.Context, dest state.Destination, text, nonce string) (string, error) {
	body := map[string]any{"content": text, "allowed_mentions": noMentions, "flags": int(discordgo.MessageFlagsSuppressEmbeds)}
	if nonce != "" {
		body["nonce"], body["enforce_nonce"] = nonce, true
	}
	raw, err := d.a.s.RequestWithBucketID("POST", discordgo.EndpointChannelMessages(dest.Channel), body, discordgo.EndpointChannelMessages(dest.Channel), discordgo.WithContext(ctx))
	if err != nil {
		if isArchivedThread(err) && d.unarchive(ctx, dest.Channel) {
			raw, err = d.a.s.RequestWithBucketID("POST", discordgo.EndpointChannelMessages(dest.Channel), body, discordgo.EndpointChannelMessages(dest.Channel), discordgo.WithContext(ctx))
		}
		if err != nil {
			return "", classify(err, true)
		}
	}
	var msg createdMessage
	if err := json.Unmarshal(raw, &msg); err != nil || msg.ID == "" {
		return "", &conversation.DeliveryError{Kind: conversation.KindAmbiguous, Err: errors.New("discord: unreadable create response")}
	}
	return msg.ID, nil
}

// Edit updates a known message with the same mention and embed policy.
func (d *Deliverer) Edit(ctx context.Context, dest state.Destination, remoteID, text string) error {
	_, err := d.a.s.ChannelMessageEditComplex(&discordgo.MessageEdit{Channel: dest.Channel, ID: remoteID, Content: &text, AllowedMentions: &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}}, Flags: discordgo.MessageFlagsSuppressEmbeds}, discordgo.WithContext(ctx))
	if err != nil {
		return classify(err, false)
	}
	return nil
}

type recentMessage struct {
	ID     string `json:"id"`
	Nonce  any    `json:"nonce"`
	Author struct {
		ID string `json:"id"`
	} `json:"author"`
}

// Reconcile scans recent destination messages for an own message carrying
// the nonce. It covers a recent window only.
func (d *Deliverer) Reconcile(ctx context.Context, dest state.Destination, nonce string) (string, bool, error) {
	if nonce == "" {
		return "", false, nil
	}
	raw, err := d.a.s.RequestWithBucketID("GET", fmt.Sprintf("%s?limit=%d", discordgo.EndpointChannelMessages(dest.Channel), reconcileWindow), nil, discordgo.EndpointChannelMessages(dest.Channel), discordgo.WithContext(ctx))
	if err != nil {
		return "", false, classify(err, false)
	}
	var msgs []recentMessage
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return "", false, err
	}
	bot := d.a.BotID()
	for _, m := range msgs {
		if m.Author.ID == bot && fmt.Sprint(m.Nonce) == nonce {
			return m.ID, true, nil
		}
	}
	return "", false, nil
}

// Allowed rechecks the destination: DMs by actor, threads by guild and
// parent channel. Unresolvable parents fail closed.
func (d *Deliverer) Allowed(dest state.Destination) bool {
	if dest.Platform != state.PlatformDiscord || dest.Installation != d.a.BotID() || !d.a.Healthy() {
		return false
	}
	if dest.DMActor != "" {
		return d.a.users.Contains(dest.DMActor)
	}
	if !d.a.guilds.Contains(dest.ThreadRoot) {
		return false
	}
	if d.a.channels.Contains(dest.Channel) {
		return true // a text channel itself (notices only)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := d.a.channel(ctx, dest.Channel)
	if err != nil || ch.Type != discordgo.ChannelTypeGuildPublicThread {
		return false
	}
	return d.a.channels.Contains(ch.ParentID)
}

// Notify posts a transient notice.
func (d *Deliverer) Notify(ctx context.Context, dest state.Destination, text string) error {
	_, err := d.Create(ctx, dest, render.SuppressDiscordMentions(text), "")
	return err
}

// Chunks renders committed text into bounded chunks.
func (d *Deliverer) Chunks(text string) []string {
	return render.Chunk(render.SuppressDiscordMentions(text), render.DiscordChunkUnits, render.DiscordMeasure)
}

// Preview renders transient text.
func (d *Deliverer) Preview(text string) string {
	return render.Preview(render.SuppressDiscordMentions(text), render.DiscordChunkUnits, render.DiscordMeasure)
}

func (d *Deliverer) unarchive(ctx context.Context, channel string) bool {
	archived := false
	_, err := d.a.s.ChannelEditComplex(channel, &discordgo.ChannelEdit{Archived: &archived}, discordgo.WithContext(ctx))
	return err == nil
}

func isArchivedThread(err error) bool {
	var rest *discordgo.RESTError
	return errors.As(err, &rest) && rest.Message != nil && rest.Message.Code == 50083
}

// classify maps SDK errors to delivery errors. DiscordGo returns a plain
// error (not a RESTError) for HTTP 502 when retries are disabled; a 502 on a
// create therefore falls through to the ambiguous branch on purpose: a
// gateway error does not prove the request never reached Discord.
func classify(err error, create bool) error {
	var rl *discordgo.RateLimitError
	if errors.As(err, &rl) {
		return &conversation.DeliveryError{Kind: conversation.KindRateLimited, RetryAfter: rl.RetryAfter, Err: errors.New("discord: rate limited")}
	}
	var rest *discordgo.RESTError
	if errors.As(err, &rest) && rest.Response != nil {
		switch rest.Response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusBadRequest:
			return &conversation.DeliveryError{Kind: conversation.KindPermanent, Err: fmt.Errorf("discord: http %d", rest.Response.StatusCode)}
		case http.StatusTooManyRequests:
			return &conversation.DeliveryError{Kind: conversation.KindRateLimited, Err: errors.New("discord: rate limited")}
		}
		return &conversation.DeliveryError{Kind: conversation.KindDefinite, Err: fmt.Errorf("discord: http %d", rest.Response.StatusCode)}
	}
	if errors.Is(err, context.Canceled) {
		return &conversation.DeliveryError{Kind: conversation.KindDefinite, Err: err}
	}
	if create {
		return &conversation.DeliveryError{Kind: conversation.KindAmbiguous, Err: errors.New("discord: transport failure")}
	}
	return &conversation.DeliveryError{Kind: conversation.KindDefinite, Err: errors.New("discord: transport failure")}
}

func safeErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 120 {
		s = s[:120]
	}
	return s
}
