package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- fixtures -------------------------------------------------------------

func validSlackConfig() Config {
	return Config{
		StateDir: "/var/lib/eino",
		Slack: Slack{
			Enabled:           true,
			TeamID:            "T12345678",
			AllowedUserIDs:    []string{"U12345678"},
			AllowedChannelIDs: []string{"C12345678"},
		},
	}
}

func validDiscordConfig() Config {
	return Config{
		StateDir: "/var/lib/eino",
		Discord: Discord{
			Enabled:           true,
			GuildIDs:          []string{"123456789012345"},
			AllowedUserIDs:    []string{"223456789012345"},
			AllowedChannelIDs: []string{"323456789012345"},
		},
	}
}

func validBothConfig() Config {
	c := validSlackConfig()
	d := validDiscordConfig()
	c.Discord = d.Discord
	return c
}

func marshal(t *testing.T, cfg Config) []byte {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return raw
}

// --- Parse: rejects ---------------------------------------------------------

func TestParseRejects(t *testing.T) {
	cases := []struct {
		name       string
		build      func() Config
		wantSubstr string
	}{
		{
			name:       "empty state_dir",
			build:      func() Config { c := validSlackConfig(); c.StateDir = ""; return c },
			wantSubstr: "state_dir is required",
		},
		{
			name:       "relative state_dir",
			build:      func() Config { c := validSlackConfig(); c.StateDir = "relative/path"; return c },
			wantSubstr: "state_dir must be an absolute path",
		},
		{
			name:       "unclean state_dir",
			build:      func() Config { c := validSlackConfig(); c.StateDir = "/tmp/../x"; return c },
			wantSubstr: "state_dir must be a clean path",
		},
		{
			name: "no platform enabled",
			build: func() Config {
				c := validSlackConfig()
				c.Slack = Slack{}
				return c
			},
			wantSubstr: "at least one platform must be enabled",
		},
		{
			name:       "slack enabled without team_id",
			build:      func() Config { c := validSlackConfig(); c.Slack.TeamID = ""; return c },
			wantSubstr: "slack.team_id must be a Slack team ID",
		},
		{
			name:       "slack enabled with empty allowed_user_ids",
			build:      func() Config { c := validSlackConfig(); c.Slack.AllowedUserIDs = nil; return c },
			wantSubstr: "slack.allowed_user_ids must not be empty",
		},
		{
			name:       "invalid slack team id",
			build:      func() Config { c := validSlackConfig(); c.Slack.TeamID = "X12345678"; return c },
			wantSubstr: "slack.team_id must be a Slack team ID",
		},
		{
			name:       "invalid slack user id",
			build:      func() Config { c := validSlackConfig(); c.Slack.AllowedUserIDs = []string{"X12345678"}; return c },
			wantSubstr: "slack.allowed_user_ids contains an invalid ID",
		},
		{
			name:       "invalid slack channel id",
			build:      func() Config { c := validSlackConfig(); c.Slack.AllowedChannelIDs = []string{"X12345678"}; return c },
			wantSubstr: "slack.allowed_channel_ids contains an invalid ID",
		},
		{
			name: "duplicate slack user ids",
			build: func() Config {
				c := validSlackConfig()
				c.Slack.AllowedUserIDs = []string{"U12345678", "U12345678"}
				return c
			},
			wantSubstr: "slack.allowed_user_ids contains a duplicate ID",
		},
		{
			name:       "discord enabled with empty allowed_user_ids",
			build:      func() Config { c := validDiscordConfig(); c.Discord.AllowedUserIDs = nil; return c },
			wantSubstr: "discord.allowed_user_ids must not be empty",
		},
		{
			name:       "discord allowed_channel_ids without guild_ids",
			build:      func() Config { c := validDiscordConfig(); c.Discord.GuildIDs = nil; return c },
			wantSubstr: "discord.guild_ids must not be empty",
		},
		{
			name:       "non-snowflake discord user id",
			build:      func() Config { c := validDiscordConfig(); c.Discord.AllowedUserIDs = []string{"abc"}; return c },
			wantSubstr: "discord.allowed_user_ids contains an invalid ID",
		},
		{
			name:       "non-snowflake discord guild id",
			build:      func() Config { c := validDiscordConfig(); c.Discord.GuildIDs = []string{"123"}; return c },
			wantSubstr: "discord.guild_ids contains an invalid ID",
		},
		{
			name:       "non-snowflake discord channel id",
			build:      func() Config { c := validDiscordConfig(); c.Discord.AllowedChannelIDs = []string{"123"}; return c },
			wantSubstr: "discord.allowed_channel_ids contains an invalid ID",
		},
		{
			name: "duplicate discord user ids",
			build: func() Config {
				c := validDiscordConfig()
				c.Discord.AllowedUserIDs = []string{"123456789012345", "123456789012345"}
				return c
			},
			wantSubstr: "discord.allowed_user_ids contains a duplicate ID",
		},
		{
			name: "slack settings present while disabled",
			build: func() Config {
				c := validDiscordConfig()
				c.Slack.TeamID = "T12345678"
				return c
			},
			wantSubstr: "slack settings are present but slack.enabled is false",
		},
		{
			name: "discord settings present while disabled",
			build: func() Config {
				c := validSlackConfig()
				c.Discord.GuildIDs = []string{"123456789012345"}
				return c
			},
			wantSubstr: "discord settings are present but discord.enabled is false",
		},
		// --- limit overrides: negative (zero is defaulted, see TestParseZeroLimitsDefaulted) ---
		{
			name:       "max_running_conversations negative",
			build:      func() Config { c := validSlackConfig(); c.Limits.MaxRunningConversations = -1; return c },
			wantSubstr: "limits.max_running_conversations must be positive",
		},
		{
			name: "max_running_conversations above ceiling",
			build: func() Config {
				c := validSlackConfig()
				c.Limits.MaxRunningConversations = CeilingRunningConversations + 1
				return c
			},
			wantSubstr: "limits.max_running_conversations exceeds the ceiling",
		},
		{
			name:       "max_queued_per_conversation negative",
			build:      func() Config { c := validSlackConfig(); c.Limits.MaxQueuedPerConversation = -1; return c },
			wantSubstr: "limits.max_queued_per_conversation must be positive",
		},
		{
			name: "max_queued_per_conversation above ceiling",
			build: func() Config {
				c := validSlackConfig()
				c.Limits.MaxQueuedPerConversation = CeilingQueuedPerConversation + 1
				return c
			},
			wantSubstr: "limits.max_queued_per_conversation exceeds the ceiling",
		},
		{
			name:       "max_pending_inbox negative",
			build:      func() Config { c := validSlackConfig(); c.Limits.MaxPendingInbox = -1; return c },
			wantSubstr: "limits.max_pending_inbox must be positive",
		},
		{
			name: "max_pending_inbox above ceiling",
			build: func() Config {
				c := validSlackConfig()
				c.Limits.MaxPendingInbox = CeilingPendingInbox + 1
				return c
			},
			wantSubstr: "limits.max_pending_inbox exceeds the ceiling",
		},
		{
			name:       "max_prompt_bytes negative",
			build:      func() Config { c := validSlackConfig(); c.Limits.MaxPromptBytes = -1; return c },
			wantSubstr: "limits.max_prompt_bytes must be positive",
		},
		{
			name: "max_prompt_bytes above ceiling",
			build: func() Config {
				c := validSlackConfig()
				c.Limits.MaxPromptBytes = CeilingPromptBytes + 1
				return c
			},
			wantSubstr: "limits.max_prompt_bytes exceeds the ceiling",
		},
		{
			name:       "model_turn_seconds negative",
			build:      func() Config { c := validSlackConfig(); c.Limits.ModelTurnSeconds = -1; return c },
			wantSubstr: "limits.model_turn_seconds must be positive",
		},
		{
			name: "model_turn_seconds above ceiling",
			build: func() Config {
				c := validSlackConfig()
				c.Limits.ModelTurnSeconds = CeilingModelTurnSeconds + 1
				return c
			},
			wantSubstr: "limits.model_turn_seconds exceeds the ceiling",
		},
		{
			name:       "platform_call_seconds negative",
			build:      func() Config { c := validSlackConfig(); c.Limits.PlatformCallSeconds = -1; return c },
			wantSubstr: "limits.platform_call_seconds must be positive",
		},
		{
			name: "platform_call_seconds above ceiling",
			build: func() Config {
				c := validSlackConfig()
				c.Limits.PlatformCallSeconds = CeilingPlatformCallSeconds + 1
				return c
			},
			wantSubstr: "limits.platform_call_seconds exceeds the ceiling",
		},
		{
			name:       "shutdown_seconds negative",
			build:      func() Config { c := validSlackConfig(); c.Limits.ShutdownSeconds = -1; return c },
			wantSubstr: "limits.shutdown_seconds must be positive",
		},
		{
			name: "shutdown_seconds above ceiling",
			build: func() Config {
				c := validSlackConfig()
				c.Limits.ShutdownSeconds = CeilingShutdownSeconds + 1
				return c
			},
			wantSubstr: "limits.shutdown_seconds exceeds the ceiling",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := marshal(t, tc.build())
			_, err := Parse(raw)
			if err == nil {
				t.Fatalf("Parse() succeeded, want error containing %q", tc.wantSubstr)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("Parse() error = %v, want errors.Is(err, ErrInvalid)", err)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("Parse() error = %q, want substring %q", err.Error(), tc.wantSubstr)
			}
		})
	}
}

// TestParseZeroLimitsDefaulted documents that a limit override of exactly 0
// is indistinguishable (via `omitempty`) from an omitted field, so
// applyDefaults substitutes the default before validation runs. Only
// negative overrides reach the "must be positive" check.
func TestParseZeroLimitsDefaulted(t *testing.T) {
	c := validSlackConfig()
	c.Limits.MaxRunningConversations = 0
	raw := marshal(t, c)
	cfg, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() unexpected error: %v", err)
	}
	if cfg.Limits.MaxRunningConversations != DefaultLimits().MaxRunningConversations {
		t.Errorf("MaxRunningConversations = %d, want default %d", cfg.Limits.MaxRunningConversations, DefaultLimits().MaxRunningConversations)
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	raw := []byte(`{"state_dir":"/var/lib/eino","slack":{"enabled":true,"team_id":"T12345678","allowed_user_ids":["U12345678"]},"discord":{},"extra_field":true}`)
	_, err := Parse(raw)
	if err == nil {
		t.Fatal("Parse() succeeded, want error for unknown field")
	}
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("Parse() error = %v, want errors.Is(err, ErrInvalid)", err)
	}
	if !strings.Contains(err.Error(), "decode config") {
		t.Errorf("Parse() error = %q, want substring %q", err.Error(), "decode config")
	}
}

func TestParseRejectsTrailingData(t *testing.T) {
	raw := marshal(t, validSlackConfig())
	raw = append(raw, []byte(`{}`)...)
	_, err := Parse(raw)
	if err == nil {
		t.Fatal("Parse() succeeded, want error for trailing data")
	}
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("Parse() error = %v, want errors.Is(err, ErrInvalid)", err)
	}
	if !strings.Contains(err.Error(), "trailing data") {
		t.Errorf("Parse() error = %q, want substring %q", err.Error(), "trailing data")
	}
}

// --- Parse: accepts ---------------------------------------------------------

func TestParseAcceptsValidConfigs(t *testing.T) {
	t.Run("slack only", func(t *testing.T) {
		cfg, err := Parse(marshal(t, validSlackConfig()))
		if err != nil {
			t.Fatalf("Parse() unexpected error: %v", err)
		}
		if !cfg.Slack.Enabled {
			t.Error("Slack.Enabled = false, want true")
		}
		if cfg.Discord.Enabled {
			t.Error("Discord.Enabled = true, want false")
		}
	})

	t.Run("discord only", func(t *testing.T) {
		cfg, err := Parse(marshal(t, validDiscordConfig()))
		if err != nil {
			t.Fatalf("Parse() unexpected error: %v", err)
		}
		if !cfg.Discord.Enabled {
			t.Error("Discord.Enabled = false, want true")
		}
		if cfg.Slack.Enabled {
			t.Error("Slack.Enabled = true, want false")
		}
	})

	t.Run("both", func(t *testing.T) {
		cfg, err := Parse(marshal(t, validBothConfig()))
		if err != nil {
			t.Fatalf("Parse() unexpected error: %v", err)
		}
		if !cfg.Slack.Enabled || !cfg.Discord.Enabled {
			t.Errorf("expected both platforms enabled, got slack=%t discord=%t", cfg.Slack.Enabled, cfg.Discord.Enabled)
		}
	})
}

func TestParseDefaultsApplied(t *testing.T) {
	cfg, err := Parse(marshal(t, validSlackConfig()))
	if err != nil {
		t.Fatalf("Parse() unexpected error: %v", err)
	}
	if cfg.Limits != DefaultLimits() {
		t.Errorf("Limits = %+v, want defaults %+v", cfg.Limits, DefaultLimits())
	}
}

func TestParseExplicitOverridesKept(t *testing.T) {
	c := validSlackConfig()
	c.Limits = Limits{
		MaxRunningConversations:  10,
		MaxQueuedPerConversation: 20,
		MaxPendingInbox:          500,
		MaxPromptBytes:           30000,
		ModelTurnSeconds:         300,
		PlatformCallSeconds:      30,
		ShutdownSeconds:          60,
	}
	cfg, err := Parse(marshal(t, c))
	if err != nil {
		t.Fatalf("Parse() unexpected error: %v", err)
	}
	if cfg.Limits != c.Limits {
		t.Errorf("Limits = %+v, want %+v", cfg.Limits, c.Limits)
	}
}

// --- LoadSecrets -------------------------------------------------------------

func getenvFromMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadSecretsMissingAPIKey(t *testing.T) {
	_, err := LoadSecrets(Config{}, getenvFromMap(nil))
	if err == nil {
		t.Fatal("LoadSecrets() succeeded, want error")
	}
	if !errors.Is(err, ErrInvalid) {
		t.Errorf("LoadSecrets() error = %v, want errors.Is(err, ErrInvalid)", err)
	}
	if !strings.Contains(err.Error(), EnvOpenCodeAPIKey) {
		t.Errorf("LoadSecrets() error = %q, want substring %q", err.Error(), EnvOpenCodeAPIKey)
	}
}

func TestLoadSecretsSlackRequiresBothTokens(t *testing.T) {
	cfg := Config{Slack: Slack{Enabled: true}}

	t.Run("missing both", func(t *testing.T) {
		env := map[string]string{EnvOpenCodeAPIKey: "key"}
		_, err := LoadSecrets(cfg, getenvFromMap(env))
		if err == nil {
			t.Fatal("LoadSecrets() succeeded, want error")
		}
		if !strings.Contains(err.Error(), EnvSlackBotToken) || !strings.Contains(err.Error(), EnvSlackAppToken) {
			t.Errorf("LoadSecrets() error = %q, want both %q and %q", err.Error(), EnvSlackBotToken, EnvSlackAppToken)
		}
	})

	t.Run("missing app token only", func(t *testing.T) {
		env := map[string]string{EnvOpenCodeAPIKey: "key", EnvSlackBotToken: "bot"}
		_, err := LoadSecrets(cfg, getenvFromMap(env))
		if err == nil {
			t.Fatal("LoadSecrets() succeeded, want error")
		}
		if !strings.Contains(err.Error(), EnvSlackAppToken) {
			t.Errorf("LoadSecrets() error = %q, want substring %q", err.Error(), EnvSlackAppToken)
		}
		if strings.Contains(err.Error(), EnvSlackBotToken) {
			t.Errorf("LoadSecrets() error = %q, should not mention satisfied %q", err.Error(), EnvSlackBotToken)
		}
	})
}

func TestLoadSecretsDiscordRequiresBotToken(t *testing.T) {
	cfg := Config{Discord: Discord{Enabled: true}}
	env := map[string]string{EnvOpenCodeAPIKey: "key"}
	_, err := LoadSecrets(cfg, getenvFromMap(env))
	if err == nil {
		t.Fatal("LoadSecrets() succeeded, want error")
	}
	if !strings.Contains(err.Error(), EnvDiscordBotToken) {
		t.Errorf("LoadSecrets() error = %q, want substring %q", err.Error(), EnvDiscordBotToken)
	}
}

func TestLoadSecretsSuccessAndTrimming(t *testing.T) {
	cfg := Config{Slack: Slack{Enabled: true}, Discord: Discord{Enabled: true}}
	env := map[string]string{
		EnvOpenCodeAPIKey:  "  key \n",
		EnvSlackBotToken:   "\tbotTok  ",
		EnvSlackAppToken:   " appTok\n",
		EnvDiscordBotToken: " discTok ",
	}
	s, err := LoadSecrets(cfg, getenvFromMap(env))
	if err != nil {
		t.Fatalf("LoadSecrets() unexpected error: %v", err)
	}
	if s.OpenCodeAPIKey != "key" {
		t.Errorf("OpenCodeAPIKey = %q, want %q", s.OpenCodeAPIKey, "key")
	}
	if s.SlackBotToken != "botTok" {
		t.Errorf("SlackBotToken = %q, want %q", s.SlackBotToken, "botTok")
	}
	if s.SlackAppToken != "appTok" {
		t.Errorf("SlackAppToken = %q, want %q", s.SlackAppToken, "appTok")
	}
	if s.DiscordBotToken != "discTok" {
		t.Errorf("DiscordBotToken = %q, want %q", s.DiscordBotToken, "discTok")
	}
}

func TestLoadSecretsNilGetenvUsesOSEnv(t *testing.T) {
	t.Setenv(EnvOpenCodeAPIKey, "from-os-env")
	s, err := LoadSecrets(Config{}, nil)
	if err != nil {
		t.Fatalf("LoadSecrets() unexpected error: %v", err)
	}
	if s.OpenCodeAPIKey != "from-os-env" {
		t.Errorf("OpenCodeAPIKey = %q, want %q", s.OpenCodeAPIKey, "from-os-env")
	}
}

// --- Redaction ---------------------------------------------------------------

func TestSecretsNeverLeakSentinel(t *testing.T) {
	const sentinel = "SENTINEL_SECRET_VALUE"
	s := Secrets{
		OpenCodeAPIKey:  sentinel,
		SlackBotToken:   sentinel,
		SlackAppToken:   sentinel,
		DiscordBotToken: sentinel,
	}

	checks := map[string]string{
		"String()":    s.String(),
		"GoString()":  s.GoString(),
		"Sprintf %v":  fmt.Sprintf("%v", s),
		"Sprintf %+v": fmt.Sprintf("%+v", s),
		"Sprintf %#v": fmt.Sprintf("%#v", s),
	}
	for name, got := range checks {
		if strings.Contains(got, sentinel) {
			t.Errorf("%s leaked secret: %q", name, got)
		}
	}

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("json.Marshal() unexpected error: %v", err)
	}
	if strings.Contains(string(b), sentinel) {
		t.Errorf("json.Marshal() leaked secret: %s", b)
	}
}

func TestConfigRedacted(t *testing.T) {
	cfg := validBothConfig()
	cfg.applyDefaults()
	out := cfg.Redacted()
	for _, want := range []string{"slack", "discord", "state_dir"} {
		if !strings.Contains(out, want) {
			t.Errorf("Redacted() = %q, want substring %q", out, want)
		}
	}
}

// --- Allowlist -----------------------------------------------------------------

func TestAllowlistContains(t *testing.T) {
	a := NewAllowlist([]string{"U1", "U2"})
	if !a.Contains("U1") {
		t.Error("Contains(U1) = false, want true")
	}
	if !a.Contains("U2") {
		t.Error("Contains(U2) = false, want true")
	}
	if a.Contains("U3") {
		t.Error("Contains(U3) = true, want false")
	}
	empty := NewAllowlist(nil)
	if empty.Contains("anything") {
		t.Error("empty Allowlist Contains() = true, want false")
	}
}

// --- Load -----------------------------------------------------------------

func TestLoad(t *testing.T) {
	t.Run("valid file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.json")
		if err := os.WriteFile(path, marshal(t, validSlackConfig()), 0o600); err != nil {
			t.Fatalf("WriteFile() unexpected error: %v", err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}
		if !cfg.Slack.Enabled {
			t.Error("Slack.Enabled = false, want true")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.json")
		_, err := Load(path)
		if err == nil {
			t.Fatal("Load() succeeded, want error")
		}
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("Load() error = %v, want errors.Is(err, ErrInvalid)", err)
		}
	})
}
