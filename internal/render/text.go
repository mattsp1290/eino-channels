// Package render turns committed model text into platform-safe, bounded
// message chunks. It never loses or duplicates bytes: joining the chunks
// without their fence-balancing markers reproduces the input.
package render

import (
	"strings"
	"unicode/utf8"
)

// Platform text budgets (including labels and escaping).
const (
	SlackChunkChars     = 3500 // Unicode characters
	DiscordChunkUnits   = 1800 // UTF-16 code units
	ContinuationNotice  = "\n… (continued)"
	ThinkingPlaceholder = "Thinking…"
)

// Measure counts the platform units of a string.
type Measure func(string) int

// SlackMeasure counts Unicode characters.
func SlackMeasure(s string) int { return utf8.RuneCountInString(s) }

// DiscordMeasure counts UTF-16 code units.
func DiscordMeasure(s string) int {
	n := 0
	for _, r := range s {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// EscapeSlack escapes Slack control characters. Because every '<' is
// escaped, Slack mention and link syntax in model output is neutralized.
func EscapeSlack(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}

// SuppressDiscordMentions neutralizes mass-mention text. Notification
// suppression is enforced by AllowedMentions on the wire; this only removes
// the visual ping syntax.
func SuppressDiscordMentions(s string) string {
	r := strings.NewReplacer("@everyone", "@​everyone", "@here", "@​here")
	return r.Replace(s)
}

// Chunk splits text into pieces whose measure never exceeds budget. It
// prefers newline boundaries, then Unicode-safe boundaries, and balances
// code fences at chunk edges.
func Chunk(text string, budget int, measure Measure) []string {
	if text == "" {
		return nil
	}
	if measure(text) <= budget {
		return []string{text}
	}
	c := &chunker{budget: budget, measure: measure, maxFence: budget / 4}
	for line := range strings.SplitAfterSeq(text, "\n") {
		if line != "" {
			c.add(line)
		}
	}
	return c.done()
}

const fenceReserve = 12 // closing "\n```" plus reopening fence line

// chunker accumulates lines into budgeted chunks while tracking the open
// code fence so every chunk renders balanced.
type chunker struct {
	budget  int
	measure Measure
	// maxFence bounds the fence line that may be reopened in every chunk;
	// a longer opener is reopened as a plain fence so model output cannot
	// multiply the number of messages.
	maxFence  int
	out       []string
	cur       strings.Builder
	curLen    int
	openFence string // the fence line currently open, e.g. "```go"
}

// limit is the budget for the current line, reserving room for fence
// balancing markers whenever a fence is open or the line is one.
func (c *chunker) limit(line string) int {
	if c.openFence != "" || isFence(line) {
		return max(c.budget-fenceReserve-c.measure(c.openFence), c.budget/2)
	}
	return c.budget
}

func (c *chunker) add(line string) {
	limit := c.limit(line)
	ll := c.measure(line)
	if ll > limit {
		for _, piece := range splitUnits(line, limit-c.curLen, limit, c.measure) {
			c.write(piece, limit)
		}
	} else {
		c.write(line, limit)
	}
	if isFence(line) {
		c.trackFence(line)
	}
}

// write appends a piece, flushing first when it would overflow.
func (c *chunker) write(piece string, limit int) {
	n := c.measure(piece)
	if c.curLen+n > limit && c.curLen > 0 {
		c.flush()
	}
	c.cur.WriteString(piece)
	c.curLen += n
}

func (c *chunker) trackFence(line string) {
	switch {
	case c.openFence != "":
		c.openFence = ""
	case c.measure(line) <= c.maxFence:
		c.openFence = strings.TrimRight(line, "\n")
	default:
		c.openFence = "```"
	}
}

// flush closes the current chunk, balancing an open fence and reopening it
// in the next chunk.
func (c *chunker) flush() {
	if c.cur.Len() == 0 {
		return
	}
	s := c.cur.String()
	if c.openFence != "" {
		if !strings.HasSuffix(s, "\n") {
			s += "\n"
		}
		s += "```\n"
	}
	c.out = append(c.out, s)
	c.cur.Reset()
	c.curLen = 0
	if c.openFence != "" {
		c.cur.WriteString(c.openFence + "\n")
		c.curLen = c.measure(c.openFence) + 1
	}
}

// done returns the chunks, closing an unterminated source fence so the last
// chunk renders.
func (c *chunker) done() []string {
	if c.cur.Len() != 0 {
		s := c.cur.String()
		if c.openFence != "" {
			if !strings.HasSuffix(s, "\n") {
				s += "\n"
			}
			s += "```"
		}
		c.out = append(c.out, s)
	}
	return c.out
}

func isFence(line string) bool {
	t := strings.TrimLeft(strings.TrimRight(line, "\n"), " ")
	return strings.HasPrefix(t, "```")
}

// splitUnits splits s into pieces: the first fits into first units, later
// pieces into rest units. Splits prefer spaces and never break runes.
func splitUnits(s string, first, rest int, measure Measure) []string {
	var out []string
	limit := first
	if limit <= 0 {
		limit = rest
	}
	for s != "" {
		if measure(s) <= limit {
			out = append(out, s)
			break
		}
		cut := 0
		lastSpace := -1
		used := 0
		for i, r := range s {
			w := measure(string(r))
			if used+w > limit {
				break
			}
			used += w
			cut = i + utf8.RuneLen(r)
			if r == ' ' {
				lastSpace = cut
			}
		}
		if cut == 0 {
			_, size := utf8.DecodeRuneInString(s)
			cut = size
		}
		if lastSpace > 0 && lastSpace < cut && measure(s[:cut]) == limit {
			cut = lastSpace
		}
		out = append(out, s[:cut])
		s = s[cut:]
		limit = rest
	}
	return out
}

// Preview bounds transient text to the first chunk and appends a
// continuation notice when the text was cut.
func Preview(text string, budget int, measure Measure) string {
	if text == "" {
		return ThinkingPlaceholder
	}
	notice := ContinuationNotice
	if measure(text) <= budget {
		return text
	}
	chunks := Chunk(text, budget-measure(notice), measure)
	if len(chunks) == 0 {
		return ThinkingPlaceholder
	}
	return strings.TrimRight(chunks[0], "\n") + notice
}
