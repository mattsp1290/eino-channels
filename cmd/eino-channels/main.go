// Command eino-channels serves Slack and Discord text conversations backed
// by eino-agent and OpenCode Go.
package main

import (
	"os"

	"github.com/mattsp1290/eino-channels/internal/app"
)

func main() {
	os.Exit(app.Main(os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}
