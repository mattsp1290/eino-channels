package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/state"
)

// associatePolicy is the per-platform evidence rule for associate-message:
// the remote ID shape, how the evidence is verified, and the audit tag.
type associatePolicy struct {
	IDHint string
	IDOK   func(string) bool
	// Verify checks the operator's evidence; it returns an exit code and a
	// message when the association must be refused.
	Verify   func(ctx context.Context, d state.Delivery, messageID string, confirm bool, getenv func(string) string, verifier MessageVerifier) (int, string)
	AuditTag string
}

var associatePolicies = map[state.Platform]associatePolicy{
	state.PlatformSlack: {
		IDHint: "--message-id must be a Slack message timestamp", IDOK: config.IsSlackTimestamp,
		Verify: func(_ context.Context, _ state.Delivery, _ string, confirm bool, _ func(string) string, _ MessageVerifier) (int, string) {
			if !confirm {
				return 2, "Slack association requires --confirm-inspected: inspect the destination and confirm the timestamp identifies the intended bot message"
			}
			return 0, ""
		},
		AuditTag: "operator-attested slack association",
	},
	state.PlatformDiscord: {
		IDHint: "--message-id must be a Discord message ID", IDOK: config.IsSnowflake,
		Verify: func(ctx context.Context, d state.Delivery, messageID string, _ bool, getenv func(string) string, verifier MessageVerifier) (int, string) {
			// Only the Discord token is needed for this verification.
			token := strings.TrimSpace(getenv(config.EnvDiscordBotToken))
			if token == "" {
				return 2, config.EnvDiscordBotToken + " is required to verify a Discord message"
			}
			if verifier == nil {
				verifier = discordVerifier{}
			}
			if err := verifier.VerifyDiscord(ctx, token, d.Destination.Channel, messageID); err != nil {
				return 1, "verification failed: " + err.Error()
			}
			return 0, ""
		},
		AuditTag: "api-verified discord association",
	},
}

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
	// THREAD/GUILD: a Slack thread timestamp, or the guild ID for Discord
	// (whose Channel column is the thread channel itself).
	fmt.Fprintf(stdout, "%-8s %-8s %-18s %-14s %-9s %-6s %-22s %s\n", "ID", "PLATFORM", "STATUS", "OP", "ATTEMPTS", "CHUNK", "CHANNEL", "THREAD/GUILD")
	for _, d := range rows {
		fmt.Fprintf(stdout, "%-8d %-8s %-18s %-14s %-9d %-6d %-22s %s\n", d.ID, d.Destination.Platform, d.Status, d.Op, d.Attempts, d.ChunkIndex, d.Destination.Channel, d.Destination.ThreadRoot)
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
		policy, ok := associatePolicies[d.Destination.Platform]
		if !ok {
			fmt.Fprintln(stderr, "unknown platform")
			return 1
		}
		if !policy.IDOK(*messageID) {
			fmt.Fprintln(stderr, policy.IDHint)
			return 2
		}
		if code, msg := policy.Verify(ctx, d, *messageID, *confirm, getenv, verifier); code != 0 {
			fmt.Fprintln(stderr, msg)
			return code
		}
		audit := policy.AuditTag + " " + time.Now().UTC().Format(time.RFC3339)
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
