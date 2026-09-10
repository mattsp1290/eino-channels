package conversation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"

	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/state"
)

func TestComposeFinalTable(t *testing.T) {
	long := string(make([]byte, config.MaxOutputBytes+10))
	for i := range long {
		_ = i
	}
	longText := ""
	for len(longText) < config.MaxOutputBytes+10 {
		longText += "abcdefghij"
	}
	cases := []struct {
		name                          string
		status                        session.RunStatus
		interrupted                   bool
		text                          string
		ok, unavailable, limit, uStop bool
		live                          string
		want, code                    string
	}{
		{"unavailable", session.RunCompleted, false, "x", true, true, false, false, "", TextUnavailable, state.CodeUnavailable},
		{"empty completed", session.RunCompleted, false, "", true, false, false, false, "", TextEmptyAnswer, state.CodeCompleted},
		{"unfinalized completed", session.RunCompleted, false, "x", false, false, false, false, "", TextEmptyAnswer, state.CodeCompleted},
		{"completed", session.RunCompleted, false, " hi ", true, false, false, false, "", "hi", state.CodeCompleted},
		{"completed over cap", session.RunCompleted, false, longText, true, false, false, false, "", longText[:config.MaxOutputBytes] + TextOutputLimit, state.CodeOutputLimit},
		{"limit hit", session.RunInterrupted, true, "", false, false, true, false, "partial", "partial" + TextOutputLimit, state.CodeOutputLimit},
		{"user stop with live", session.RunInterrupted, true, "", false, false, false, true, "partial ", "partial" + TextStopped, state.CodeInterrupted},
		{"user stop empty", session.RunInterrupted, true, "", false, false, false, true, "", TextStoppedEmpty, state.CodeInterrupted},
		{"restart with live", session.RunInterrupted, false, "", false, false, false, false, "p", "p" + TextInterrupted, state.CodeInterrupted},
		{"restart empty", session.RunInterrupted, false, "", false, false, false, false, "", TextInterruptedNil, state.CodeInterrupted},
		{"failed", session.RunFailed, false, "", false, false, false, false, "p", TextFailed, state.CodeFailed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, code := composeFinal(runtime.Result{Status: c.status, Interrupted: c.interrupted}, c.text, c.ok, c.unavailable, c.limit, c.uStop, c.live)
			if got != c.want || code != c.code {
				t.Fatalf("got (%q…, %s) want (%q…, %s)", got[:min(len(got), 40)], code, c.want[:min(len(c.want), 40)], c.code)
			}
		})
	}
	_ = long
}

func newBareService() *Service {
	return &Service{limits: config.DefaultLimits(), active: map[string]struct{}{}, parked: map[string]time.Time{}, handles: map[string]runtime.Handle{}, stopFlags: map[string]bool{}, ingests: map[string]uint64{}, limiterLast: map[state.Destination]time.Time{}, spacing: 50 * time.Millisecond}
}

// park refuses a decision made on stale information: an ingest that
// arrived after the runner observed the route wins.
func TestParkRefusesStaleDecision(t *testing.T) {
	s := newBareService()
	seen := s.ingestSeq("r")
	if !s.park("r", time.Minute, seen) {
		t.Fatal("fresh park refused")
	}
	if _, ok := s.parked["r"]; !ok {
		t.Fatal("not parked")
	}
	s.unpark("r")
	if _, ok := s.parked["r"]; ok {
		t.Fatal("unpark left the park")
	}
	if s.park("r", time.Minute, seen) {
		t.Fatal("stale park accepted after an ingest")
	}
	if !s.rescan {
		t.Fatal("ingest did not request a rescan")
	}
	// Counter pruning drops only idle routes and never below the cap.
	for i := 0; i < maxIngestEntries+5; i++ {
		s.unpark(string(rune('a'+i%26)) + string(rune(i)))
	}
	if len(s.ingests) > maxIngestEntries+1 {
		t.Fatalf("ingests not pruned: %d", len(s.ingests))
	}
}

// throttle gives the spacing slot back when the wait is cut short.
func TestThrottleRollsBackOnCancel(t *testing.T) {
	s := newBareService()
	dest := state.Destination{Platform: state.PlatformSlack, Channel: "C1", DMActor: "U1"}
	if err := s.throttle(context.Background(), dest); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.throttle(ctx, dest); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	key := dest
	key.DMActor = ""
	if s.limiterLast[key].After(time.Now()) {
		t.Fatal("cancelled wait kept the future slot")
	}
	if _, ok := s.limiterLast[dest]; ok {
		t.Fatal("actor must not be part of the spacing key")
	}
}
