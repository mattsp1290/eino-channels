// Package slack is the Slack Socket Mode ingress and Web API delivery
// adapter. It validates identity, mentions and allowlists before handing
// normalized input to the conversation service, and acknowledges every
// transport envelope only after the runnable input is durably stored.
package slack

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/render"
	"github.com/mattsp1290/eino-channels/internal/state"
)

// Options configure the adapter.
type Options struct {
	Config   config.Slack
	BotToken string
	AppToken string
	Service  *conversation.Service
	Logger   *slog.Logger
	// APIURL and HTTPClient are test seams for a fake Web API.
	APIURL     string
	HTTPClient *http.Client
	// PersistDeadline bounds the durable write before an acknowledgement.
	PersistDeadline time.Duration
}

// Adapter is the Slack transport.
type Adapter struct {
	cfg      config.Slack
	users    config.Allowlist
	channels config.Allowlist
	api      *slack.Client
	sm       *socketmode.Client
	svc      *conversation.Service
	log      *slog.Logger
	deadline time.Duration

	// identity is written once by setIdentity and read by the event loop;
	// guarded because Run and tests may call Identity from another goroutine.
	idMu      sync.Mutex
	botUserID string
	mention   *regexp.Regexp
	healthy   atomic.Bool
}

// New builds the adapter without network calls.
func New(opts Options) (*Adapter, error) {
	if opts.BotToken == "" || opts.AppToken == "" {
		return nil, errors.New("slack: bot and app tokens required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.PersistDeadline <= 0 {
		opts.PersistDeadline = time.Second // target well inside Slack's 3 s ack window
	}
	apiOpts := []slack.Option{slack.OptionAppLevelToken(opts.AppToken), slack.OptionDebug(false)}
	if opts.APIURL != "" {
		apiOpts = append(apiOpts, slack.OptionAPIURL(opts.APIURL))
	}
	if opts.HTTPClient != nil {
		apiOpts = append(apiOpts, slack.OptionHTTPClient(opts.HTTPClient))
	}
	api := slack.New(opts.BotToken, apiOpts...)
	a := &Adapter{
		cfg: opts.Config, users: config.NewAllowlist(opts.Config.AllowedUserIDs), channels: config.NewAllowlist(opts.Config.AllowedChannelIDs),
		api: api, sm: socketmode.New(api, socketmode.OptionDebug(false)), svc: opts.Service, log: opts.Logger, deadline: opts.PersistDeadline,
	}
	return a, nil
}

// Attach binds the conversation service. It must be called before Run.
func (a *Adapter) Attach(svc *conversation.Service) { a.svc = svc }

// Identity validates the bot and team identity through auth.test. It
// never logs the returned identities.
func (a *Adapter) Identity(ctx context.Context) error {
	resp, err := a.api.AuthTestContext(ctx)
	if err != nil {
		return fmt.Errorf("slack: auth.test failed: %w", classifyCall(err))
	}
	if resp.TeamID != a.cfg.TeamID {
		return errors.New("slack: bot token belongs to a different team than configured")
	}
	if resp.UserID == "" {
		return errors.New("slack: auth.test returned no bot user")
	}
	a.setIdentity(resp.UserID)
	return nil
}

func (a *Adapter) setIdentity(botUserID string) {
	a.idMu.Lock()
	a.botUserID = botUserID
	a.mention = regexp.MustCompile(`<@` + regexp.QuoteMeta(botUserID) + `(\|[^>]*)?>`)
	a.idMu.Unlock()
	a.healthy.Store(true)
}

func (a *Adapter) identity() (string, *regexp.Regexp) {
	a.idMu.Lock()
	defer a.idMu.Unlock()
	return a.botUserID, a.mention
}

// Healthy reports whether the adapter is connected with valid auth.
func (a *Adapter) Healthy() bool { return a.healthy.Load() }

// Run validates identity, connects Socket Mode and processes envelopes
// until ctx is canceled. It returns when the connection loop ends.
func (a *Adapter) Run(ctx context.Context) error {
	if a.svc == nil {
		return errors.New("slack: conversation service not attached")
	}
	if err := a.Identity(ctx); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() { errCh <- a.sm.RunContext(ctx) }()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			a.healthy.Store(false)
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("slack: socket mode stopped: %w", err)
		case evt := <-a.sm.Events:
			a.HandleEvent(ctx, evt)
		}
	}
}

// HandleEvent processes one Socket Mode event. It is exported for tests
// that feed synthetic envelopes.
func (a *Adapter) HandleEvent(ctx context.Context, evt socketmode.Event) {
	switch evt.Type {
	case socketmode.EventTypeInvalidAuth:
		a.healthy.Store(false)
		a.log.Error("slack: authentication invalid; adapter halted")
	case socketmode.EventTypeConnected:
		a.healthy.Store(true)
	case socketmode.EventTypeConnectionError, socketmode.EventTypeDisconnect:
		a.log.Warn("slack: connection interrupted; SDK will reconnect")
	case socketmode.EventTypeEventsAPI:
		payload, ok := evt.Data.(slackevents.EventsAPIEvent)
		if !ok || evt.Request == nil {
			return
		}
		if a.handleEventsAPI(ctx, payload) {
			a.ack(evt.Request)
		}
	default:
		if evt.Request != nil && evt.Request.EnvelopeID != "" {
			a.ack(evt.Request)
		}
	}
}

func (a *Adapter) ack(req *socketmode.Request) {
	if err := a.sm.Ack(*req); err != nil {
		a.log.Warn("slack: ack failed; Slack may redeliver", "error", err.Error())
	}
}

// handleEventsAPI returns true when the envelope should be acknowledged.
// Runnable input is acknowledged only after durable storage succeeds.
func (a *Adapter) handleEventsAPI(ctx context.Context, payload slackevents.EventsAPIEvent) bool {
	botUserID, mention := a.identity()
	if botUserID == "" || mention == nil {
		return false // identity not established yet; let Slack retry
	}
	if payload.Type != slackevents.CallbackEvent || payload.TeamID != a.cfg.TeamID {
		return true
	}
	// Slack Connect / external shared channels are out of scope; the flag
	// can appear on the outer event or on the callback envelope.
	if payload.IsExtSharedChannel {
		return true
	}
	if cb, ok := payload.Data.(*slackevents.EventsAPICallbackEvent); ok && cb.IsExtSharedChannel {
		return true
	}
	in, ok := a.normalize(payload.InnerEvent, botUserID, mention)
	if !ok {
		return true
	}
	pctx, cancel := context.WithTimeout(ctx, a.deadline)
	defer cancel()
	resp, err := a.svc.Ingest(pctx, in)
	if err != nil {
		a.log.Warn("slack: durable ingest failed; not acknowledging", "error", err.Error())
		return false
	}
	if resp.Notice != "" {
		a.svc.Notify(in.Route, resp.Notice)
	}
	return true
}

// message is the platform-neutral view of one Slack event, built by one
// constructor per event type so it is explicit which fields each type
// populates.
type message struct {
	User, Text, TS, ThreadTS, Channel, BotID string
	UserTeam, SourceTeam                     string
	HasFiles, Edited, DM                     bool
}

func fromAppMention(e *slackevents.AppMentionEvent) message {
	return message{User: e.User, Text: e.Text, TS: e.TimeStamp, ThreadTS: e.ThreadTimeStamp, Channel: e.Channel, BotID: e.BotID, UserTeam: e.UserTeam, SourceTeam: e.SourceTeam, HasFiles: len(e.Files) != 0, Edited: e.Edited != nil, DM: strings.HasPrefix(e.Channel, "D")}
}

// fromDirectMessage accepts only plain new messages in a DM channel.
func fromDirectMessage(e *slackevents.MessageEvent) (message, bool) {
	if e.ChannelType != "im" || e.SubType != "" || e.IsEdited() {
		return message{}, false
	}
	m := message{User: e.User, Text: e.Text, TS: e.TimeStamp, ThreadTS: e.ThreadTimeStamp, Channel: e.Channel, BotID: e.BotID, UserTeam: e.UserTeam, SourceTeam: e.SourceTeam, DM: true}
	if e.Message != nil {
		if e.Message.Hidden || e.Message.BotID != "" {
			return message{}, false
		}
		m.HasFiles = len(e.Message.Files) != 0
	}
	return m, true
}

// normalize converts a callback inner event into a validated Inbound.
func (a *Adapter) normalize(inner slackevents.EventsAPIInnerEvent, botUserID string, mention *regexp.Regexp) (state.Inbound, bool) {
	var m message
	switch e := inner.Data.(type) {
	case *slackevents.AppMentionEvent:
		m = fromAppMention(e)
	case *slackevents.MessageEvent:
		var ok bool
		if m, ok = fromDirectMessage(e); !ok {
			return state.Inbound{}, false
		}
	default:
		return state.Inbound{}, false
	}
	return a.validate(m, botUserID, mention)
}

// validate applies identity, allowlist, mention and routing rules.
func (a *Adapter) validate(m message, botUserID string, mention *regexp.Regexp) (state.Inbound, bool) {
	if m.BotID != "" || m.User == "" || m.User == botUserID || m.Edited || m.TS == "" || m.Channel == "" {
		return state.Inbound{}, false
	}
	if m.UserTeam != "" && m.UserTeam != a.cfg.TeamID || m.SourceTeam != "" && m.SourceTeam != a.cfg.TeamID {
		return state.Inbound{}, false // Slack Connect / external identity is out of scope
	}
	if !a.users.Contains(m.User) {
		return state.Inbound{}, false
	}
	route := state.Route{Platform: state.PlatformSlack, Installation: a.cfg.TeamID, Channel: m.Channel}
	if m.DM {
		route.DMActor = m.User
	} else {
		if !a.channels.Contains(m.Channel) {
			return state.Inbound{}, false
		}
		if !mention.MatchString(m.Text) {
			return state.Inbound{}, false // shared-thread follow-ups require a fresh mention
		}
		route.ThreadRoot = m.ThreadTS
		if route.ThreadRoot == "" {
			route.ThreadRoot = m.TS
		}
	}
	text := strings.TrimSpace(mention.ReplaceAllString(m.Text, ""))
	if text == "" {
		return state.Inbound{}, false // blank or attachment-only
	}
	return state.Inbound{Route: route, MessageID: m.TS, Actor: m.User, ActorLabel: m.User, Content: text, FilesNotice: m.HasFiles, ReceivedAt: time.Now()}, true
}

// --- delivery -----------------------------------------------------------------

// Deliverer exposes the Web API delivery seam.
type Deliverer struct{ a *Adapter }

// Deliverer returns the delivery implementation.
func (a *Adapter) Deliverer() *Deliverer { return &Deliverer{a: a} }

func (d *Deliverer) options(dest state.Destination, text string) []slack.MsgOption {
	opts := []slack.MsgOption{slack.MsgOptionText(text, false), slack.MsgOptionParse(false), slack.MsgOptionLinkNames(false), slack.MsgOptionDisableLinkUnfurl(), slack.MsgOptionDisableMediaUnfurl()}
	if dest.ThreadRoot != "" {
		opts = append(opts, slack.MsgOptionTS(dest.ThreadRoot))
	}
	return opts
}

// Create posts a message (threaded when the destination has a root).
func (d *Deliverer) Create(ctx context.Context, dest state.Destination, text, _ string) (string, error) {
	_, ts, err := d.a.api.PostMessageContext(ctx, dest.Channel, d.options(dest, text)...)
	if err != nil {
		return "", classifyCreate(err)
	}
	return ts, nil
}

// Edit updates a known message.
func (d *Deliverer) Edit(ctx context.Context, dest state.Destination, remoteID, text string) error {
	_, _, _, err := d.a.api.UpdateMessageContext(ctx, dest.Channel, remoteID, slack.MsgOptionText(text, false), slack.MsgOptionParse(false), slack.MsgOptionLinkNames(false))
	if err != nil {
		return classifyCall(err)
	}
	return nil
}

// Reconcile is unavailable in v1: no history scope is granted. Ambiguous
// creates require operator resolution.
func (d *Deliverer) Reconcile(context.Context, state.Destination, string) (string, bool, error) {
	return "", false, nil
}

// Allowed rechecks the destination against the allowlists. Socket Mode
// health is not authorization: Web API delivery is independent of it.
func (d *Deliverer) Allowed(_ context.Context, dest state.Destination, subject string) (bool, error) {
	if dest.Platform != state.PlatformSlack || dest.Installation != d.a.cfg.TeamID {
		return false, nil
	}
	if subject != "" {
		return d.a.users.Contains(subject), nil
	}
	return d.a.channels.Contains(dest.Channel), nil
}

// Notify posts a transient notice.
func (d *Deliverer) Notify(ctx context.Context, dest state.Destination, text string) error {
	_, err := d.Create(ctx, dest, render.EscapeSlack(text), "")
	return err
}

// Chunks escapes and splits committed text.
func (d *Deliverer) Chunks(text string) []string {
	return render.Chunk(render.EscapeSlack(text), render.SlackChunkChars, render.SlackMeasure)
}

// Preview renders transient text.
func (d *Deliverer) Preview(text string) string {
	return render.Preview(render.EscapeSlack(text), render.SlackChunkChars, render.SlackMeasure)
}

// classifyCreate classifies a failed create: an unknown outcome is ambiguous
// because the message may have landed and Slack has no nonce to check.
func classifyCreate(err error) error { return classify(err, conversation.CreateOutcome) }

// classifyCall classifies any other failed Web API call.
func classifyCall(err error) error { return classify(err, conversation.CallOutcome) }

func classify(err error, policy conversation.OutcomePolicy) error {
	var rl *slack.RateLimitedError
	if errors.As(err, &rl) {
		return &conversation.DeliveryError{Kind: conversation.KindRateLimited, RetryAfter: rl.RetryAfter, Err: err}
	}
	var api slack.SlackErrorResponse
	if errors.As(err, &api) {
		switch api.Err {
		case "fatal_error", "internal_error":
			// Slack documents these as possibly partially applied.
			if policy == conversation.CreateOutcome {
				return &conversation.DeliveryError{Kind: conversation.KindAmbiguous, Err: errors.New("slack: " + api.Err)}
			}
			return &conversation.DeliveryError{Kind: conversation.KindDefinite, Err: errors.New("slack: " + api.Err)}
		case "channel_not_found", "not_in_channel", "is_archived", "invalid_auth", "account_inactive", "token_revoked", "missing_scope", "msg_too_long", "restricted_action", "message_not_found", "cant_update_message", "not_authed", "no_permission", "ekm_access_denied":
			return &conversation.DeliveryError{Kind: conversation.KindPermanent, Err: errors.New("slack: " + api.Err)}
		}
		return &conversation.DeliveryError{Kind: conversation.KindDefinite, Err: errors.New("slack: " + api.Err)}
	}
	return conversation.TransportFailure("slack", policy)
}
