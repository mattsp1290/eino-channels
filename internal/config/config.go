// Package config loads and validates the eino-channels service configuration.
//
// Credentials never live in the configuration file. They are read from the
// process environment by LoadSecrets and are never included in Redacted output.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Fixed provider selection. These are constants, not configuration.
const (
	ProviderID = "opencode-go"
	ModelID    = "deepseek-v4-flash"
	Protocol   = "chat_completions"
	AgentName  = "eino-channels"

	// SystemPrompt is the application-owned conversation prompt. It makes no
	// tool claims.
	SystemPrompt = "You are a helpful assistant answering in a chat conversation. Reply in plain text with concise, direct answers. You cannot run tools, browse, read files, or take actions; if asked, say so briefly."
)

// Environment variable names for credentials.
const (
	EnvOpenCodeAPIKey  = "OPENCODE_API_KEY"
	EnvSlackBotToken   = "SLACK_BOT_TOKEN"
	EnvSlackAppToken   = "SLACK_APP_TOKEN"
	EnvDiscordBotToken = "DISCORD_BOT_TOKEN"
)

// Limits are the bounded runtime defaults. Only the exposed fields may be
// overridden through the configuration file; each override must be positive
// and at or below its hard ceiling. An absent or explicit zero value selects
// the default (the fields are omitempty, so the two are indistinguishable).
type Limits struct {
	MaxRunningConversations  int `json:"max_running_conversations,omitempty"`
	MaxQueuedPerConversation int `json:"max_queued_per_conversation,omitempty"`
	MaxPendingInbox          int `json:"max_pending_inbox,omitempty"`
	MaxPromptBytes           int `json:"max_prompt_bytes,omitempty"`
	ModelTurnSeconds         int `json:"model_turn_seconds,omitempty"`
	PlatformCallSeconds      int `json:"platform_call_seconds,omitempty"`
	ShutdownSeconds          int `json:"shutdown_seconds,omitempty"`
}

// Hard ceilings for the exposed limits.
const (
	CeilingRunningConversations  = 16
	CeilingQueuedPerConversation = 32
	CeilingPendingInbox          = 1024
	CeilingPromptBytes           = 64 << 10
	CeilingModelTurnSeconds      = 600
	CeilingPlatformCallSeconds   = 60
	CeilingShutdownSeconds       = 120
)

// limitSpec describes one tunable limit: its config field, accessor,
// default and ceiling. Every limit is listed exactly once here; defaults,
// validation and the redacted summary iterate this table.
type limitSpec struct {
	Field   string
	Get     func(*Limits) *int
	Default int
	Ceiling int
}

var limitSpecs = []limitSpec{
	{"limits.max_running_conversations", func(l *Limits) *int { return &l.MaxRunningConversations }, 4, CeilingRunningConversations},
	{"limits.max_queued_per_conversation", func(l *Limits) *int { return &l.MaxQueuedPerConversation }, 8, CeilingQueuedPerConversation},
	{"limits.max_pending_inbox", func(l *Limits) *int { return &l.MaxPendingInbox }, 256, CeilingPendingInbox},
	{"limits.max_prompt_bytes", func(l *Limits) *int { return &l.MaxPromptBytes }, 16 << 10, CeilingPromptBytes},
	{"limits.model_turn_seconds", func(l *Limits) *int { return &l.ModelTurnSeconds }, 120, CeilingModelTurnSeconds},
	{"limits.platform_call_seconds", func(l *Limits) *int { return &l.PlatformCallSeconds }, 10, CeilingPlatformCallSeconds},
	{"limits.shutdown_seconds", func(l *Limits) *int { return &l.ShutdownSeconds }, 10, CeilingShutdownSeconds},
}

// DefaultLimits returns the v1 defaults.
func DefaultLimits() Limits {
	var l Limits
	for _, spec := range limitSpecs {
		*spec.Get(&l) = spec.Default
	}
	return l
}

// Internal limits that are not exposed for tuning in v1.
const (
	AgentLeaseSeconds     = 5
	MaxHistoryTurns       = 100
	MaxHistoryBytes       = 256 << 10
	MaxOutputBytes        = 32 << 10
	MaxProviderReqBytes   = 1 << 20
	MaxOutputTokens       = 4096
	PreviewCoalesceMillis = 1200
	DeliveryMaxAttempts   = 5
	DeliveryWindowMinutes = 10
)

// Slack holds the Slack adapter configuration.
type Slack struct {
	Enabled           bool     `json:"enabled"`
	TeamID            string   `json:"team_id,omitempty"`
	AllowedChannelIDs []string `json:"allowed_channel_ids,omitempty"`
	AllowedUserIDs    []string `json:"allowed_user_ids,omitempty"`
}

// Discord holds the Discord adapter configuration.
type Discord struct {
	Enabled           bool     `json:"enabled"`
	GuildIDs          []string `json:"guild_ids,omitempty"`
	AllowedChannelIDs []string `json:"allowed_channel_ids,omitempty"`
	AllowedUserIDs    []string `json:"allowed_user_ids,omitempty"`
}

// Config is the validated service configuration.
type Config struct {
	StateDir string  `json:"state_dir"`
	Slack    Slack   `json:"slack"`
	Discord  Discord `json:"discord"`
	Limits   Limits  `json:"limits,omitempty"`
}

// Secrets holds credentials read from the environment. It is never serialized.
type Secrets struct {
	OpenCodeAPIKey  string
	SlackBotToken   string
	SlackAppToken   string
	DiscordBotToken string
}

// String prevents accidental credential printing.
func (Secrets) String() string { return "config.Secrets{<redacted>}" }

// GoString prevents accidental credential printing.
func (Secrets) GoString() string { return "config.Secrets{<redacted>}" }

// MarshalJSON prevents accidental credential serialization.
func (Secrets) MarshalJSON() ([]byte, error) { return []byte(`"<redacted>"`), nil }

// ErrInvalid marks configuration validation failures.
var ErrInvalid = errors.New("invalid configuration")

var (
	slackTeamRe    = regexp.MustCompile(`^T[A-Z0-9]{2,}$`)
	slackChannelRe = regexp.MustCompile(`^[CG][A-Z0-9]{2,}$`)
	slackUserRe    = regexp.MustCompile(`^[UW][A-Z0-9]{2,}$`)
	snowflakeRe    = regexp.MustCompile(`^[0-9]{15,22}$`)
	slackTSRe      = regexp.MustCompile(`^[0-9]{10}\.[0-9]{6}$`)
)

// IsSnowflake reports whether id has the shape of a Discord snowflake.
func IsSnowflake(id string) bool { return snowflakeRe.MatchString(id) }

// IsSlackTimestamp reports whether id has the shape of a Slack message ts.
func IsSlackTimestamp(id string) bool { return slackTSRe.MatchString(id) }

// Load reads and validates a configuration file.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("%w: read config: %v", ErrInvalid, err)
	}
	return Parse(raw)
}

// Parse decodes and validates configuration JSON.
func Parse(raw []byte) (Config, error) {
	var cfg Config
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("%w: decode config: %v", ErrInvalid, err)
	}
	if decoder.More() {
		return Config{}, fmt.Errorf("%w: trailing data after configuration object", ErrInvalid)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	for _, spec := range limitSpecs {
		if v := spec.Get(&c.Limits); *v == 0 {
			*v = spec.Default
		}
	}
}

// Validate checks every invariant the service depends on.
func (c Config) Validate() error {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if c.StateDir == "" {
		add("state_dir is required")
	} else if !filepath.IsAbs(c.StateDir) {
		add("state_dir must be an absolute path")
	} else if filepath.Clean(c.StateDir) != c.StateDir {
		add("state_dir must be a clean path")
	}
	if !c.Slack.Enabled && !c.Discord.Enabled {
		add("at least one platform must be enabled")
	}
	if c.Slack.Enabled {
		if !slackTeamRe.MatchString(c.Slack.TeamID) {
			add("slack.team_id must be a Slack team ID")
		}
		if len(c.Slack.AllowedUserIDs) == 0 {
			add("slack.allowed_user_ids must not be empty when slack is enabled")
		}
		checkIDs(&problems, "slack.allowed_user_ids", c.Slack.AllowedUserIDs, slackUserRe)
		checkIDs(&problems, "slack.allowed_channel_ids", c.Slack.AllowedChannelIDs, slackChannelRe)
	} else if c.Slack.TeamID != "" || len(c.Slack.AllowedChannelIDs) != 0 || len(c.Slack.AllowedUserIDs) != 0 {
		add("slack settings are present but slack.enabled is false")
	}
	if c.Discord.Enabled {
		if len(c.Discord.AllowedUserIDs) == 0 {
			add("discord.allowed_user_ids must not be empty when discord is enabled")
		}
		if len(c.Discord.AllowedChannelIDs) != 0 && len(c.Discord.GuildIDs) == 0 {
			add("discord.guild_ids must not be empty when discord.allowed_channel_ids is set")
		}
		checkIDs(&problems, "discord.allowed_user_ids", c.Discord.AllowedUserIDs, snowflakeRe)
		checkIDs(&problems, "discord.guild_ids", c.Discord.GuildIDs, snowflakeRe)
		checkIDs(&problems, "discord.allowed_channel_ids", c.Discord.AllowedChannelIDs, snowflakeRe)
	} else if len(c.Discord.GuildIDs) != 0 || len(c.Discord.AllowedChannelIDs) != 0 || len(c.Discord.AllowedUserIDs) != 0 {
		add("discord settings are present but discord.enabled is false")
	}
	for _, spec := range limitSpecs {
		checkLimit(&problems, spec.Field, *spec.Get(&c.Limits), spec.Ceiling)
	}
	if len(problems) != 0 {
		return fmt.Errorf("%w: %s", ErrInvalid, strings.Join(problems, "; "))
	}
	return nil
}

func checkIDs(problems *[]string, field string, values []string, re *regexp.Regexp) {
	seen := map[string]struct{}{}
	for _, v := range values {
		if !re.MatchString(v) {
			*problems = append(*problems, fmt.Sprintf("%s contains an invalid ID", field))
			return
		}
		if _, dup := seen[v]; dup {
			*problems = append(*problems, fmt.Sprintf("%s contains a duplicate ID", field))
			return
		}
		seen[v] = struct{}{}
	}
}

func checkLimit(problems *[]string, field string, value, ceiling int) {
	if value <= 0 {
		*problems = append(*problems, fmt.Sprintf("%s must be positive", field))
	} else if value > ceiling {
		*problems = append(*problems, fmt.Sprintf("%s exceeds the ceiling %d", field, ceiling))
	}
}

// LoadSecrets reads credentials from the environment for the enabled
// adapters. getenv is injectable for tests; nil uses os.Getenv.
func LoadSecrets(cfg Config, getenv func(string) string) (Secrets, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	var missing []string
	s := Secrets{OpenCodeAPIKey: strings.TrimSpace(getenv(EnvOpenCodeAPIKey))}
	if s.OpenCodeAPIKey == "" {
		missing = append(missing, EnvOpenCodeAPIKey)
	}
	if cfg.Slack.Enabled {
		s.SlackBotToken = strings.TrimSpace(getenv(EnvSlackBotToken))
		s.SlackAppToken = strings.TrimSpace(getenv(EnvSlackAppToken))
		if s.SlackBotToken == "" {
			missing = append(missing, EnvSlackBotToken)
		} else if !strings.HasPrefix(s.SlackBotToken, "xoxb-") {
			return Secrets{}, fmt.Errorf("%w: %s must be a bot token (xoxb-…)", ErrInvalid, EnvSlackBotToken)
		}
		if s.SlackAppToken == "" {
			missing = append(missing, EnvSlackAppToken)
		} else if !strings.HasPrefix(s.SlackAppToken, "xapp-") {
			return Secrets{}, fmt.Errorf("%w: %s must be an app-level token (xapp-…)", ErrInvalid, EnvSlackAppToken)
		}
	}
	if cfg.Discord.Enabled {
		s.DiscordBotToken = strings.TrimSpace(getenv(EnvDiscordBotToken))
		if s.DiscordBotToken == "" {
			missing = append(missing, EnvDiscordBotToken)
		}
	}
	if len(missing) != 0 {
		return Secrets{}, fmt.Errorf("%w: missing environment variables: %s", ErrInvalid, strings.Join(missing, ", "))
	}
	return s, nil
}

// Redacted returns a printable summary of the non-secret configuration.
func (c Config) Redacted() string {
	var b strings.Builder
	fmt.Fprintf(&b, "state_dir: %s\n", c.StateDir)
	fmt.Fprintf(&b, "provider: %s model: %s protocol: %s\n", ProviderID, ModelID, Protocol)
	fmt.Fprintf(&b, "slack: enabled=%t team=%s channels=%d users=%d\n", c.Slack.Enabled, c.Slack.TeamID, len(c.Slack.AllowedChannelIDs), len(c.Slack.AllowedUserIDs))
	fmt.Fprintf(&b, "discord: enabled=%t guilds=%d channels=%d users=%d\n", c.Discord.Enabled, len(c.Discord.GuildIDs), len(c.Discord.AllowedChannelIDs), len(c.Discord.AllowedUserIDs))
	l := c.Limits
	fmt.Fprintf(&b, "limits: running=%d queued=%d inbox=%d prompt_bytes=%d turn_s=%d call_s=%d shutdown_s=%d\n", l.MaxRunningConversations, l.MaxQueuedPerConversation, l.MaxPendingInbox, l.MaxPromptBytes, l.ModelTurnSeconds, l.PlatformCallSeconds, l.ShutdownSeconds)
	return b.String()
}

// Allowlist is a finite set of platform IDs.
type Allowlist map[string]struct{}

// NewAllowlist builds a set from IDs.
func NewAllowlist(ids []string) Allowlist {
	a := make(Allowlist, len(ids))
	for _, id := range ids {
		a[id] = struct{}{}
	}
	return a
}

// Contains reports membership.
func (a Allowlist) Contains(id string) bool {
	_, ok := a[id]
	return ok
}
