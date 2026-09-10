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
	"sync/atomic"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
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
		opts.PersistDeadline = 2 * time.Second
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
		return fmt.Errorf("slack: auth.test failed: %w", classify(err, false))
	}
	if resp.TeamID != a.cfg.TeamID {
		return errors.New("slack: bot token belongs to a different team than configured")
	}
	if resp.UserID == "" {
		return errors.New("slack: auth.test returned no bot user")
	}
	a.setIdentity(resp.TeamID, resp.UserID)
	return nil
}

func (a *Adapter) setIdentity(_, botUserID string) {
	a.botUserID = botUserID
	a.mention = regexp.MustCompile(`<@` + regexp.QuoteMeta(botUserID) + `(\|[^>]*)?>`)
	a.healthy.Store(true)
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
	if a.botUserID == "" {
		return false // identity not established yet; let Slack retry
	}
	if payload.Type != slackevents.CallbackEvent || payload.TeamID != a.cfg.TeamID {
		return true
	}
	if cb, ok := payload.Data.(*slackevents.EventsAPICallbackEvent); payload.IsExtSharedChannel || ok && cb.IsExtSharedChannel {
		return true
	}
	in, ok := a.normalize(payload.InnerEvent)
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
		a.svc.Notify(in.Route.Destination(), resp.Notice)
	}
	return true
}

// normalize converts a callback inner event into a validated Inbound.
func (a *Adapter) normalize(inner slackevents.EventsAPIInnerEvent) (state.Inbound, bool) {
	var user, text, ts, threadTS, channel, botID, userTeam, sourceTeam string
	var files, edited, dm bool
	switch e := inner.Data.(type) {
	case *slackevents.AppMentionEvent:
		user, text, ts, threadTS, channel, botID = e.User, e.Text, e.TimeStamp, e.ThreadTimeStamp, e.Channel, e.BotID
		userTeam, sourceTeam = e.UserTeam, e.SourceTeam
		files, edited = len(e.Files) != 0, e.Edited != nil
		dm = strings.HasPrefix(channel, "D")
	case *slackevents.MessageEvent:
		if e.ChannelType != "im" || e.SubType != "" || e.IsEdited() {
			return state.Inbound{}, false
		}
		user, text, ts, threadTS, channel, botID = e.User, e.Text, e.TimeStamp, e.ThreadTimeStamp, e.Channel, e.BotID
		userTeam, sourceTeam = e.UserTeam, e.SourceTeam
		if e.Message != nil {
			files = len(e.Message.Files) != 0
			if e.Message.Hidden || e.Message.BotID != "" {
				return state.Inbound{}, false
			}
		}
		dm = true
	default:
		return state.Inbound{}, false
	}
	if botID != "" || user == "" || user == a.botUserID || edited || ts == "" || channel == "" {
		return state.Inbound{}, false
	}
	if userTeam != "" && userTeam != a.cfg.TeamID || sourceTeam != "" && sourceTeam != a.cfg.TeamID {
		return state.Inbound{}, false // Slack Connect / external identity is out of scope
	}
	if !a.users.Contains(user) {
		return state.Inbound{}, false
	}
	route := state.Route{Platform: state.PlatformSlack, Installation: a.cfg.TeamID, Channel: channel}
	if dm {
		route.DMActor = user
	} else {
		if !a.channels.Contains(channel) {
			return state.Inbound{}, false
		}
		if !a.mention.MatchString(text) {
			return state.Inbound{}, false // shared-thread follow-ups require a fresh mention
		}
		route.ThreadRoot = threadTS
		if route.ThreadRoot == "" {
			route.ThreadRoot = ts
		}
	}
	text = strings.TrimSpace(a.mention.ReplaceAllString(text, ""))
	if text == "" {
		return state.Inbound{}, false // blank or attachment-only
	}
	return state.Inbound{Route: route, MessageID: ts, Actor: user, ActorLabel: user, Content: text, FilesNotice: files, ReceivedAt: time.Now()}, true
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
		return "", classify(err, true)
	}
	return ts, nil
}

// Edit updates a known message.
func (d *Deliverer) Edit(ctx context.Context, dest state.Destination, remoteID, text string) error {
	_, _, _, err := d.a.api.UpdateMessageContext(ctx, dest.Channel, remoteID, slack.MsgOptionText(text, false), slack.MsgOptionParse(false), slack.MsgOptionLinkNames(false))
	if err != nil {
		return classify(err, false)
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
func (d *Deliverer) Allowed(_ context.Context, dest state.Destination) (bool, error) {
	if dest.Platform != state.PlatformSlack || dest.Installation != d.a.cfg.TeamID {
		return false, nil
	}
	if dest.DMActor != "" {
		return d.a.users.Contains(dest.DMActor), nil
	}
	return d.a.channels.Contains(dest.Channel), nil
}

// Notify posts a transient notice.
func (d *Deliverer) Notify(ctx context.Context, dest state.Destination, text string) error {
	_, err := d.Create(ctx, dest, d.escape(text), "")
	return err
}

func (d *Deliverer) escape(text string) string { return conversationEscape(text) }

// Chunks escapes and splits committed text.
func (d *Deliverer) Chunks(text string) []string {
	return chunkSlack(d.escape(text))
}

// Preview renders transient text.
func (d *Deliverer) Preview(text string) string {
	return previewSlack(d.escape(text))
}

// classify maps SDK errors to delivery errors. Network failures after a
// create are ambiguous; everything else is definite unless permanent.
func classify(err error, create bool) error {
	var rl *slack.RateLimitedError
	if errors.As(err, &rl) {
		return &conversation.DeliveryError{Kind: conversation.KindRateLimited, RetryAfter: rl.RetryAfter, Err: err}
	}
	var api slack.SlackErrorResponse
	if errors.As(err, &api) {
		switch api.Err {
		case "channel_not_found", "not_in_channel", "is_archived", "invalid_auth", "account_inactive", "token_revoked", "missing_scope", "msg_too_long", "restricted_action", "message_not_found", "cant_update_message", "not_authed", "no_permission", "ekm_access_denied":
			return &conversation.DeliveryError{Kind: conversation.KindPermanent, Err: errors.New("slack: " + api.Err)}
		}
		return &conversation.DeliveryError{Kind: conversation.KindDefinite, Err: errors.New("slack: " + api.Err)}
	}
	if create {
		// Cancellation or a transport failure after a create was sent does
		// not prove Slack never processed it; Slack has no nonce to check.
		return &conversation.DeliveryError{Kind: conversation.KindAmbiguous, Err: errors.New("slack: transport failure")}
	}
	return &conversation.DeliveryError{Kind: conversation.KindDefinite, Err: errors.New("slack: transport failure")}
}
