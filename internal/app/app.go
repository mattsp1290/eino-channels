// Package app assembles the eino-channels service and implements the CLI:
// serve, check-config and the operator delivery commands.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/conversation"
	"github.com/mattsp1290/eino-channels/internal/discord"
	"github.com/mattsp1290/eino-channels/internal/slack"
	"github.com/mattsp1290/eino-channels/internal/state"
)

// Version is the release identity carried in the provider User-Agent.
const Version = "0.1.0"

// UserAgent is the truthful provider identity.
const UserAgent = "eino-channels/" + Version

const usage = `usage:
  eino-channels serve --config <path>
  eino-channels check-config --config <path>
  eino-channels delivery list --config <path> [--limit N]
  eino-channels delivery resolve --config <path> --id <id> --action associate-message --message-id <id> [--confirm-inspected]
  eino-channels delivery resolve --config <path> --id <id> --action resend
  eino-channels version
`

// Main runs the CLI and returns the exit code.
func Main(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if getenv == nil {
		getenv = os.Getenv
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, UserAgent)
		return 0
	case "check-config":
		return checkConfig(args[1:], stdout, stderr, getenv)
	case "serve":
		return serve(args[1:], stderr, getenv, logger)
	case "delivery":
		return deliveryCommand(args[1:], stdout, stderr, getenv)
	}
	fmt.Fprint(stderr, usage)
	return 2
}

func configFlag(fs *flag.FlagSet) *string {
	return fs.String("config", "", "path to the configuration file")
}

func loadConfig(fs *flag.FlagSet, args []string, path *string, stderr io.Writer) (config.Config, bool) {
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return config.Config{}, false
	}
	if *path == "" {
		fmt.Fprintln(stderr, "--config is required")
		return config.Config{}, false
	}
	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(stderr, err.Error())
		return config.Config{}, false
	}
	return cfg, true
}

// checkConfig validates the file and the credential environment without
// any network or inference. It never prints secrets.
func checkConfig(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("check-config", flag.ContinueOnError)
	path := configFlag(fs)
	cfg, ok := loadConfig(fs, args, path, stderr)
	if !ok {
		return 2
	}
	if _, err := config.LoadSecrets(cfg, getenv); err != nil {
		fmt.Fprintln(stderr, err.Error())
		return 2
	}
	fmt.Fprint(stdout, cfg.Redacted())
	fmt.Fprintln(stdout, "credentials: present")
	fmt.Fprintln(stdout, "ok")
	return 0
}

func serve(args []string, stderr io.Writer, getenv func(string) string, logger *slog.Logger) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	path := configFlag(fs)
	cfg, ok := loadConfig(fs, args, path, stderr)
	if !ok {
		return 2
	}
	secrets, err := config.LoadSecrets(cfg, getenv)
	if err != nil {
		fmt.Fprintln(stderr, err.Error())
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	code, err := Serve(ctx, cfg, secrets, logger)
	if err != nil {
		logger.Error("serve failed", "error", err.Error())
	}
	return code
}

// Serve runs the service until ctx is canceled or an adapter fails fatally.
// It returns the process exit code.
func Serve(ctx context.Context, cfg config.Config, secrets config.Secrets, logger *slog.Logger) (int, error) {
	st, err := state.Open(ctx, cfg.StateDir, state.Options{})
	if err != nil {
		return 1, err
	}
	defer func() { _ = st.Close() }()

	bridge, err := agentbridge.New(ctx, agentbridge.Options{Store: st.Agent(), OwnerID: "eino-channels-" + randomHex(8), APIKey: secrets.OpenCodeAPIKey, UserAgent: UserAgent})
	if err != nil {
		return 1, err
	}
	deliverers := map[state.Platform]conversation.Deliverer{}
	var slackAdapter *slack.Adapter
	var discordAdapter *discord.Adapter
	if cfg.Slack.Enabled {
		slackAdapter, err = slack.New(slack.Options{Config: cfg.Slack, BotToken: secrets.SlackBotToken, AppToken: secrets.SlackAppToken, Logger: logger})
		if err != nil {
			return 1, err
		}
		deliverers[state.PlatformSlack] = slackAdapter.Deliverer()
	}
	if cfg.Discord.Enabled {
		discordAdapter, err = discord.New(discord.Options{Config: cfg.Discord, Token: secrets.DiscordBotToken, Store: st, Logger: logger, UserAgent: UserAgent})
		if err != nil {
			return 1, err
		}
		deliverers[state.PlatformDiscord] = discordAdapter.Deliverer()
	}
	svc, err := conversation.New(conversation.Options{Limits: cfg.Limits, Store: st, Bridge: bridge, Deliverers: deliverers, Logger: logger})
	if err != nil {
		return 1, err
	}
	// The service context outlives the adapters: ingress stops first, then
	// the service settles owned work within its shutdown budget.
	svcCtx, svcCancel := context.WithCancel(context.Background())
	defer svcCancel()
	svc.Start(svcCtx)
	adapterCtx, adapterCancel := context.WithCancel(ctx)
	defer adapterCancel()

	fatal := make(chan error, 2)
	if slackAdapter != nil {
		slackAdapter.Attach(svc)
		go func() { fatal <- slackAdapter.Run(adapterCtx) }()
	}
	if discordAdapter != nil {
		discordAdapter.Attach(svc)
		go func() { fatal <- discordAdapter.Run(adapterCtx) }()
	}
	logger.Info("eino-channels serving", "version", Version, "slack", cfg.Slack.Enabled, "discord", cfg.Discord.Enabled)

	var exitErr error
	select {
	case <-ctx.Done():
	case err := <-fatal:
		if err != nil {
			exitErr = err
			logger.Error("adapter stopped", "error", err.Error())
			// One platform failing does not stop the other unless it is the only one.
			if len(deliverers) > 1 {
				select {
				case <-ctx.Done():
				case err2 := <-fatal:
					if err2 != nil {
						logger.Error("adapter stopped", "error", err2.Error())
					}
				}
			}
		}
	}
	adapterCancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.ShutdownSeconds)*time.Second)
	defer shutdownCancel()
	code := 0
	if err := svc.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown incomplete; recovery records retained", "error", err.Error())
		code = 3
	}
	svcCancel()
	if err := bridge.Close(shutdownCtx); err != nil {
		logger.Warn("watch close", "error", err.Error())
	}
	if exitErr != nil && code == 0 {
		code = 1
	}
	return code, exitErr
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ErrDaemonRunning is returned when an operator command finds the lock held.
var ErrDaemonRunning = errors.New("the service is running; stop it before resolving deliveries")
