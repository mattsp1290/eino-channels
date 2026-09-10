// Package redact renders errors and text for logs and chat safely: known
// credential shapes are removed and text is truncated on rune boundaries.
// It is the single home of that policy; callers must not keep local copies.
package redact

import (
	"regexp"
	"unicode/utf8"
)

const maxLogBytes = 200

var tokenShapes = regexp.MustCompile(`xox[abp]-[A-Za-z0-9-]+|xapp-[A-Za-z0-9-]+|Bot [A-Za-z0-9._-]{20,}|Bearer [A-Za-z0-9._-]{16,}`)

// Err renders an error for logs: known token shapes are replaced and the
// text is truncated. It does not make arbitrary error bodies safe; callers
// still avoid logging provider or platform bodies.
func Err(err error) string {
	if err == nil {
		return ""
	}
	return TruncateUTF8(tokenShapes.ReplaceAllString(err.Error(), "<redacted>"), maxLogBytes)
}

// TruncateUTF8 cuts s to at most n bytes on a rune boundary.
func TruncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
