package redact

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestErrRedactsTokenShapesAndTruncates(t *testing.T) {
	err := errors.New("auth failed for xoxb-SENTINEL-123 and xapp-1-SENTINEL and Bot SENTINEL_TOKEN_VALUE_1234567890 and Bearer SENTINEL_BEARER_VALUE_1")
	got := Err(err)
	if strings.Contains(got, "SENTINEL") || strings.Count(got, "<redacted>") != 4 {
		t.Fatalf("got %q", got)
	}
	long := errors.New(strings.Repeat("é", 300))
	out := Err(long)
	if len(out) > maxLogBytes || !utf8.ValidString(out) {
		t.Fatalf("len=%d valid=%v", len(out), utf8.ValidString(out))
	}
	if Err(nil) != "" {
		t.Fatal("nil error must render empty")
	}
}

func TestTruncateUTF8(t *testing.T) {
	s := "aé😀b"
	for n := 0; n <= len(s); n++ {
		out := TruncateUTF8(s, n)
		if len(out) > n || !utf8.ValidString(out) || !strings.HasPrefix(s, out) {
			t.Fatalf("n=%d out=%q", n, out)
		}
	}
}
