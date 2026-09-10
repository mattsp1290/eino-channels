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
	const fenceReserve = 12 // closing "\n```" plus reopening fence line
	// A fence line longer than this share of the budget is not tracked as a
	// fence: re-opening it in every chunk would let model output multiply
	// the number of messages.
	maxFence := budget / 4
	var chunks []string
	var cur strings.Builder
	curLen := 0
	openFence := "" // the fence line currently open, e.g. "```go"
	flush := func() {
		if cur.Len() == 0 {
			return
		}
		s := cur.String()
		if openFence != "" {
			if !strings.HasSuffix(s, "\n") {
				s += "\n"
			}
			s += "```\n"
		}
		chunks = append(chunks, s)
		cur.Reset()
		curLen = 0
		if openFence != "" {
			cur.WriteString(openFence + "\n")
			curLen = measure(openFence) + 1
		}
	}
	lines := strings.SplitAfter(text, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		limit := budget
		if openFence != "" || isFence(line) && measure(line) <= maxFence {
			limit = max(budget-fenceReserve-measure(openFence), budget/2)
		}
		ll := measure(line)
		if curLen+ll > limit && curLen > 0 {
			flush()
		}
		if ll > limit {
			for _, piece := range splitUnits(line, limit-curLen, limit, measure) {
				if curLen+measure(piece) > limit && curLen > 0 {
					flush()
				}
				cur.WriteString(piece)
				curLen += measure(piece)
			}
		} else {
			cur.WriteString(line)
			curLen += ll
		}
		if isFence(line) {
			switch {
			case openFence != "":
				openFence = ""
			case measure(line) <= maxFence:
				openFence = strings.TrimRight(line, "\n")
			}
		}
	}
	if cur.Len() != 0 {
		s := cur.String()
		if openFence != "" {
			// Unterminated fence in the source: close it so the last chunk renders.
			if !strings.HasSuffix(s, "\n") {
				s += "\n"
			}
			s += "```"
		}
		chunks = append(chunks, s)
	}
	return chunks
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
