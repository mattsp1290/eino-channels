package render

import (
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"
)

// --- Measure -----------------------------------------------------------------

func TestSlackMeasureCountsRunes(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
	}{
		{"ascii", "hello", 5},
		{"latin accented", "héllo", 5},
		{"single emoji", "😀", 1},
		{"empty", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SlackMeasure(tc.in); got != tc.want {
				t.Errorf("SlackMeasure(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestDiscordMeasureCountsUTF16Units(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
	}{
		{"ascii", "hello", 5},
		{"latin accented single unit", "é", 1},
		{"astral emoji is two units", "😀", 2},
		{"mixed", "a😀é", 1 + 2 + 1},
		{"empty", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DiscordMeasure(tc.in); got != tc.want {
				t.Errorf("DiscordMeasure(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// --- Escaping ------------------------------------------------------------------

func TestEscapeSlack(t *testing.T) {
	in := `A & B <tag> <@U123> <!channel>`
	got := EscapeSlack(in)
	if strings.Contains(got, "<") {
		t.Errorf("EscapeSlack(%q) = %q, still contains '<'", in, got)
	}
	if !strings.Contains(got, "&amp;") {
		t.Errorf("EscapeSlack(%q) = %q, missing &amp; escape", in, got)
	}
	if !strings.Contains(got, "&lt;") || !strings.Contains(got, "&gt;") {
		t.Errorf("EscapeSlack(%q) = %q, missing &lt;/&gt; escapes", in, got)
	}
}

func TestSuppressDiscordMentions(t *testing.T) {
	in := "ping @everyone and also @here now"
	got := SuppressDiscordMentions(in)
	if strings.Contains(got, "@everyone") {
		t.Errorf("SuppressDiscordMentions(%q) = %q, still contains literal @everyone", in, got)
	}
	if strings.Contains(got, "@here") {
		t.Errorf("SuppressDiscordMentions(%q) = %q, still contains literal @here", in, got)
	}
}

// --- Chunk: basic properties ---------------------------------------------------

func TestChunkEmptyReturnsNil(t *testing.T) {
	got := Chunk("", 100, SlackMeasure)
	if got != nil {
		t.Errorf("Chunk(\"\", ...) = %#v, want nil", got)
	}
}

func TestChunkWithinBudgetReturnsSingleChunk(t *testing.T) {
	text := "hello world"
	got := Chunk(text, 100, SlackMeasure)
	if len(got) != 1 {
		t.Fatalf("Chunk() len = %d, want 1", len(got))
	}
	if got[0] != text {
		t.Errorf("Chunk()[0] = %q, want %q", got[0], text)
	}
}

func assertChunksValid(t *testing.T, chunks []string, budget int, measure Measure) {
	t.Helper()
	for i, c := range chunks {
		if !utf8.ValidString(c) {
			t.Errorf("chunk %d is not valid UTF-8: %q", i, c)
		}
		if m := measure(c); m > budget {
			t.Errorf("chunk %d measure = %d, exceeds budget %d", i, m, budget)
		}
	}
}

func TestChunkManyShortLines(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 100; i++ {
		b.WriteString("line number ")
		b.WriteString(strings.Repeat("x", i%7))
		b.WriteString("\n")
	}
	text := b.String()
	budget := 30
	chunks := Chunk(text, budget, SlackMeasure)
	assertChunksValid(t, chunks, budget, SlackMeasure)
	if joined := strings.Join(chunks, ""); joined != text {
		t.Errorf("joined chunks != input\n got=%q\nwant=%q", joined, text)
	}
}

func TestChunkVeryLongLineWithoutSpaces(t *testing.T) {
	text := strings.Repeat("a", 500) + "\n"
	budget := 20
	chunks := Chunk(text, budget, SlackMeasure)
	assertChunksValid(t, chunks, budget, SlackMeasure)
	if joined := strings.Join(chunks, ""); joined != text {
		t.Errorf("joined chunks != input\n got=%q\nwant=%q", joined, text)
	}
}

func TestChunkVeryLongMultibyteLineWithoutSpaces(t *testing.T) {
	text := strings.Repeat("日", 300)
	budget := 15
	chunks := Chunk(text, budget, SlackMeasure)
	assertChunksValid(t, chunks, budget, SlackMeasure)
	if joined := strings.Join(chunks, ""); joined != text {
		t.Errorf("joined chunks != input\n got=%q\nwant=%q", joined, text)
	}
}

func TestChunkLongLineWithSpaces(t *testing.T) {
	text := strings.Repeat("word ", 200) + "\n"
	budget := 25
	chunks := Chunk(text, budget, SlackMeasure)
	assertChunksValid(t, chunks, budget, SlackMeasure)
	if joined := strings.Join(chunks, ""); joined != text {
		t.Errorf("joined chunks != input\n got=%q\nwant=%q", joined, text)
	}
}

func TestChunkMultibyteTextDiscordMeasure(t *testing.T) {
	text := strings.Repeat("日本語テスト😀🎉👍", 15)
	budget := 50
	chunks := Chunk(text, budget, DiscordMeasure)
	assertChunksValid(t, chunks, budget, DiscordMeasure)
	if joined := strings.Join(chunks, ""); joined != text {
		t.Errorf("joined chunks != input\n got=%q\nwant=%q", joined, text)
	}
}

func TestChunkPropertyRandomized(t *testing.T) {
	pool := []rune(
		"abcxyz0129 \n.,!~-_" +
			"éñü" +
			"日本語中文" +
			"😀😃🎉🚀👍",
	)
	seeds := []int64{1, 2, 3, 4, 5}
	measures := []struct {
		name    string
		measure Measure
	}{
		{"slack", SlackMeasure},
		{"discord", DiscordMeasure},
	}

	for _, seed := range seeds {
		rnd := rand.New(rand.NewSource(seed))
		n := 50 + rnd.Intn(450)
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteRune(pool[rnd.Intn(len(pool))])
		}
		text := b.String()
		if text == "" {
			continue
		}

		for _, mc := range measures {
			budget := 10 + rnd.Intn(191)
			chunks := Chunk(text, budget, mc.measure)
			var joined strings.Builder
			for i, c := range chunks {
				if !utf8.ValidString(c) {
					t.Fatalf("seed %d measure %s: chunk %d not valid UTF-8: %q", seed, mc.name, i, c)
				}
				if m := mc.measure(c); m > budget {
					t.Fatalf("seed %d measure %s: chunk %d measure %d exceeds budget %d", seed, mc.name, i, m, budget)
				}
				joined.WriteString(c)
			}
			if joined.String() != text {
				t.Fatalf("seed %d measure %s: join mismatch\n got=%q\nwant=%q", seed, mc.name, joined.String(), text)
			}
		}
	}
}

// --- Chunk: code fences ----------------------------------------------------------

// stripFenceMarkerLines removes every line that is exactly "```" or exactly
// fenceLine, leaving other content (and newlines) intact. Used to compare
// text reconstructed from chunks against the original input, tolerating the
// synthetic close/reopen fence-marker lines Chunk inserts at split points.
func stripFenceMarkerLines(s, fenceLine string) string {
	lines := strings.SplitAfter(s, "\n")
	var b strings.Builder
	for _, line := range lines {
		trimmed := strings.TrimRight(line, "\n")
		if trimmed == "```" || trimmed == fenceLine {
			continue
		}
		b.WriteString(line)
	}
	return b.String()
}

func countFenceLines(chunk string) int {
	n := 0
	for _, line := range strings.Split(chunk, "\n") {
		if strings.HasPrefix(strings.TrimLeft(line, " "), "```") {
			n++
		}
	}
	return n
}

func TestChunkCodeFenceBalancing(t *testing.T) {
	const fenceLine = "```go"
	text := "intro line one\n" +
		"intro line two\n" +
		fenceLine + "\n" +
		strings.Repeat("code line filler content here\n", 30) +
		"```\n" +
		"outro line\n"

	budget := 60
	chunks := Chunk(text, budget, SlackMeasure)
	if len(chunks) < 2 {
		t.Fatalf("expected the fenced block to be split into multiple chunks, got %d", len(chunks))
	}

	assertChunksValid(t, chunks, budget, SlackMeasure)

	for i, c := range chunks {
		if n := countFenceLines(c); n%2 != 0 {
			t.Errorf("chunk %d has an odd number of fence lines (%d), fences unbalanced: %q", i, n, c)
		}
	}

	joined := strings.Join(chunks, "")
	gotStripped := stripFenceMarkerLines(joined, fenceLine)
	wantStripped := stripFenceMarkerLines(text, fenceLine)
	if gotStripped != wantStripped {
		t.Errorf("reconstruction mismatch after stripping fence marker lines\n got=%q\nwant=%q", gotStripped, wantStripped)
	}
}

func TestChunkUnterminatedFenceClosesLastChunk(t *testing.T) {
	const fenceLine = "```go"
	text := fenceLine + "\n" + strings.Repeat("code filler content line here\n", 30)

	budget := 60
	chunks := Chunk(text, budget, SlackMeasure)
	if len(chunks) == 0 {
		t.Fatal("expected at least one chunk")
	}
	assertChunksValid(t, chunks, budget, SlackMeasure)

	last := chunks[len(chunks)-1]
	if !strings.HasSuffix(last, "```") {
		t.Errorf("last chunk does not end with a closing fence: %q", last)
	}
}

// --- Preview -----------------------------------------------------------------

func TestPreviewEmptyReturnsPlaceholder(t *testing.T) {
	got := Preview("", 100, SlackMeasure)
	if got != ThinkingPlaceholder {
		t.Errorf("Preview(\"\", ...) = %q, want %q", got, ThinkingPlaceholder)
	}
}

func TestPreviewShortTextReturnsItself(t *testing.T) {
	text := "short reply"
	got := Preview(text, 100, SlackMeasure)
	if got != text {
		t.Errorf("Preview(%q, ...) = %q, want unchanged", text, got)
	}
}

func TestPreviewLongTextTruncatedWithNotice(t *testing.T) {
	text := strings.Repeat("word ", 1000)
	budget := 60
	got := Preview(text, budget, SlackMeasure)
	if m := SlackMeasure(got); m > budget {
		t.Errorf("Preview() measure = %d, exceeds budget %d", m, budget)
	}
	if !strings.HasSuffix(got, ContinuationNotice) {
		t.Errorf("Preview() = %q, want suffix %q", got, ContinuationNotice)
	}
}

// A model-controlled fence line must not multiply the number of chunks.
func TestChunkOverlongFenceLineDoesNotAmplify(t *testing.T) {
	budget := 1800
	fence := "```" + strings.Repeat("x", 900) + "\n"
	body := strings.Repeat("line of code\n", 400)
	text := fence + body + "```\n"
	chunks := Chunk(text, budget, DiscordMeasure)
	total := 0
	for _, c := range chunks {
		if DiscordMeasure(c) > budget {
			t.Fatalf("chunk over budget: %d", DiscordMeasure(c))
		}
		total += len(c)
	}
	if want := len(text)/budget + 2; len(chunks) > want*2 {
		t.Fatalf("amplified: %d chunks for %d bytes", len(chunks), len(text))
	}
	if total > 2*len(text) {
		t.Fatalf("output %d bytes for %d input bytes", total, len(text))
	}
	// Text after the closing fence must not be rendered inside a code block.
	after := Chunk(text+strings.Repeat("prose line\n", 300), budget, DiscordMeasure)
	last := after[len(after)-1]
	if strings.HasPrefix(last, "```") || strings.Count(last, "```")%2 != 0 {
		t.Fatalf("fence state inverted after overlong opener: %q", last[:min(len(last), 80)])
	}
	// Ordinary fences still balance and reopen, with a floor on the budget.
	normal := "```go\n" + strings.Repeat("fmt.Println(1)\n", 300) + "```\n"
	for _, c := range Chunk(normal, 200, SlackMeasure) {
		if SlackMeasure(c) > 200 {
			t.Fatalf("over budget: %d", SlackMeasure(c))
		}
		n := 0
		for _, line := range strings.Split(strings.TrimRight(c, "\n"), "\n") {
			if strings.HasPrefix(line, "```") {
				n++
			}
		}
		if n%2 != 0 {
			t.Fatalf("unbalanced fence in chunk %q", c)
		}
	}
}
