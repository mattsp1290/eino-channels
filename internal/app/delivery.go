package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/state"
)

var (
	slackTSRe   = regexp.MustCompile(`^[0-9]{10}\.[0-9]{6}$`)
	snowflakeRe = regexp.MustCompile(`^[0-9]{15,22}$`)
)

// deliveryCommand implements `delivery list` and `delivery resolve`. Both
// require the daemon stopped (exclusive lock) and never expose prompt or
// answer bodies, tokens, or model calls.
func deliveryCommand(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "list":
		return deliveryList(args[1:], stdout, stderr)
	case "resolve":
		return deliveryResolve(args[1:], stdout, stderr, getenv, nil)
	}
	fmt.Fprint(stderr, usage)
	return 2
}

func openForOperator(ctx context.Context, cfg config.Config, stderr io.Writer) (*state.Store, bool) {
	st, err := state.Open(ctx, cfg.StateDir, state.Options{SkipAgentDB: true})
	if errors.Is(err, state.ErrLocked) {
		fmt.Fprintln(stderr, ErrDaemonRunning.Error())
		return nil, false
	}
	if err != nil {
		fmt.Fprintln(stderr, err.Error())
		return nil, false
	}
	return st, true
}

func deliveryList(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("delivery list", flag.ContinueOnError)
	path := configFlag(fs)
	limit := fs.Int("limit", 50, "maximum rows")
	cfg, ok := loadConfig(fs, args, path, stderr)
	if !ok {
		return 2
	}
	if *limit <= 0 || *limit > 500 {
		fmt.Fprintln(stderr, "--limit must be between 1 and 500")
		return 2
	}
	ctx := context.Background()
	st, ok := openForOperator(ctx, cfg, stderr)
	if !ok {
		return 1
	}
	defer func() { _ = st.Close() }()
	rows, err := st.ListDeliveries(ctx, *limit)
	if err != nil {
		fmt.Fprintln(stderr, "listing failed:", err.Error())
		return 1
	}
	fmt.Fprintf(stdout, "%-8s %-8s %-18s %-14s %-9s %-9s\n", "ID", "PLATFORM", "STATUS", "OP", "ATTEMPTS", "CHUNK")
	for _, d := range rows {
		fmt.Fprintf(stdout, "%-8d %-8s %-18s %-14s %-9d %-9d\n", d.ID, d.Destination.Platform, d.Status, d.Op, d.Attempts, d.ChunkIndex)
	}
	return 0
}

// MessageVerifier checks operator-supplied remote message evidence.
type MessageVerifier interface {
	VerifyDiscord(ctx context.Context, token, channelID, messageID string) error
}

type discordVerifier struct{}

// VerifyDiscord confirms through the API that the message exists in the
// stored destination and was authored by this bot.
func (discordVerifier) VerifyDiscord(ctx context.Context, token, channelID, messageID string) error {
	s, err := discordgo.New("Bot " + token)
	if err != nil {
		return err
	}
	s.ShouldRetryOnRateLimit = false
	s.MaxRestRetries = 0
	me, err := s.User("@me", discordgo.WithContext(ctx))
	if err != nil {
		return errors.New("discord identity lookup failed")
	}
	msg, err := s.ChannelMessage(channelID, messageID, discordgo.WithContext(ctx))
	if err != nil {
		return errors.New("discord message lookup failed")
	}
	if msg.Author == nil || msg.Author.ID != me.ID || msg.ChannelID != channelID {
		return errors.New("message is not an own message in the stored destination")
	}
	return nil
}

func deliveryResolve(args []string, stdout, stderr io.Writer, getenv func(string) string, verifier MessageVerifier) int {
	fs := flag.NewFlagSet("delivery resolve", flag.ContinueOnError)
	path := configFlag(fs)
	idFlag := fs.String("id", "", "delivery ID")
	action := fs.String("action", "", "associate-message or resend")
	messageID := fs.String("message-id", "", "remote message ID (associate-message)")
	confirm := fs.Bool("confirm-inspected", false, "operator attests the destination was inspected (Slack)")
	cfg, ok := loadConfig(fs, args, path, stderr)
	if !ok {
		return 2
	}
	id, err := strconv.ParseInt(*idFlag, 10, 64)
	if err != nil || id <= 0 {
		fmt.Fprintln(stderr, "--id must be a positive integer")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, ok := openForOperator(ctx, cfg, stderr)
	if !ok {
		return 1
	}
	defer func() { _ = st.Close() }()
	d, err := st.GetDelivery(ctx, id)
	if err != nil {
		fmt.Fprintln(stderr, "delivery not found")
		return 1
	}
	if d.Status != state.DeliveryAmbiguous && d.Status != state.DeliveryFailed {
		fmt.Fprintf(stderr, "delivery %d is %s; only ambiguous_create or failed deliveries can be resolved\n", id, d.Status)
		return 1
	}
	switch *action {
	case "resend":
		if err := st.Resend(ctx, id, "operator resend "+time.Now().UTC().Format(time.RFC3339)+"; duplicate external message possible"); err != nil {
			fmt.Fprintln(stderr, "resend failed:", err.Error())
			return 1
		}
		fmt.Fprintf(stdout, "delivery %d scheduled for resend at next serve start (a duplicate external message is possible)\n", id)
		return 0
	case "associate-message":
		if *messageID == "" {
			fmt.Fprintln(stderr, "--message-id is required for associate-message")
			return 2
		}
		var audit string
		switch d.Destination.Platform {
		case state.PlatformSlack:
			if !slackTSRe.MatchString(*messageID) {
				fmt.Fprintln(stderr, "--message-id must be a Slack message timestamp")
				return 2
			}
			if !*confirm {
				fmt.Fprintln(stderr, "Slack association requires --confirm-inspected: inspect the destination and confirm the timestamp identifies the intended bot message")
				return 2
			}
			audit = "operator-attested slack association " + time.Now().UTC().Format(time.RFC3339)
		case state.PlatformDiscord:
			if !snowflakeRe.MatchString(*messageID) {
				fmt.Fprintln(stderr, "--message-id must be a Discord message ID")
				return 2
			}
			secrets, err := config.LoadSecrets(cfg, getenv)
			if err != nil {
				fmt.Fprintln(stderr, err.Error())
				return 2
			}
			if verifier == nil {
				verifier = discordVerifier{}
			}
			if err := verifier.VerifyDiscord(ctx, secrets.DiscordBotToken, d.Destination.Channel, *messageID); err != nil {
				fmt.Fprintln(stderr, "verification failed:", err.Error())
				return 1
			}
			audit = "api-verified discord association " + time.Now().UTC().Format(time.RFC3339)
		default:
			fmt.Fprintln(stderr, "unknown platform")
			return 1
		}
		if err := st.AssociateMessage(ctx, id, *messageID, audit); err != nil {
			fmt.Fprintln(stderr, "association failed:", err.Error())
			return 1
		}
		fmt.Fprintf(stdout, "delivery %d associated; the latest committed revision will be applied as an edit at next serve start\n", id)
		return 0
	}
	fmt.Fprintln(stderr, "--action must be associate-message or resend")
	return 2
}
