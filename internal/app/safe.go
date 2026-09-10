package app

import (
	"regexp"
	"unicode/utf8"
)

var tokenShapes = regexp.MustCompile(`xox[abp]-[A-Za-z0-9-]+|xapp-[A-Za-z0-9-]+|Bot [A-Za-z0-9._-]{20,}|Bearer [A-Za-z0-9._-]{16,}`)

// safeErr renders an error for logs: known token shapes are redacted and
// the text is truncated on a rune boundary so an SDK error that embeds a
// response body cannot flood or leak through the log.
func safeErr(err error) string {
	if err == nil {
		return ""
	}
	msg := tokenShapes.ReplaceAllString(err.Error(), "<redacted>")
	if len(msg) > 200 {
		cut := 200
		for cut > 0 && !utf8.RuneStart(msg[cut]) {
			cut--
		}
		msg = msg[:cut]
	}
	return msg
}
