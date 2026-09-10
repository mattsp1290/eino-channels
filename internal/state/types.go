package state

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// Platform identifies a transport.
type Platform string

// Supported platforms.
const (
	PlatformSlack   Platform = "slack"
	PlatformDiscord Platform = "discord"
)

// Route identifies one conversation. Slack DM = team + DM channel + actor;
// Slack shared = team + channel + thread root. Discord DM = bot + DM channel
// + actor; Discord shared = bot + guild + thread channel (Channel carries
// the thread channel ID, ThreadRoot the guild ID).
type Route struct {
	Platform     Platform
	Installation string
	Channel      string
	ThreadRoot   string
	DMActor      string
}

// IsDM reports whether the route is a private direct-message route.
func (r Route) IsDM() bool { return r.DMActor != "" }

// Key returns the canonical versioned length-delimited encoding.
func (r Route) Key() string {
	parts := []string{string(r.Platform), r.Installation, r.Channel, r.ThreadRoot, r.DMActor}
	var b strings.Builder
	b.WriteString("v1")
	for _, p := range parts {
		fmt.Fprintf(&b, "|%d:%s", len(p), p)
	}
	return b.String()
}

// Destination is the immutable delivery address of a route. Authorization
// subjects (the DM actor) are not part of the address; see Route.Subject.
type Destination struct {
	Platform     Platform
	Installation string
	Channel      string
	ThreadRoot   string
}

// Destination returns the delivery address for the route.
func (r Route) Destination() Destination {
	return Destination{Platform: r.Platform, Installation: r.Installation, Channel: r.Channel, ThreadRoot: r.ThreadRoot}
}

// Subject returns the actor that authorization rechecks use for a private
// route, or "" for a shared route.
func (r Route) Subject() string { return r.DMActor }

// Conversation is the durable route record.
type Conversation struct {
	Route            Route
	Generation       int64
	RuntimeSessionID string
	CreatorActor     string
	CreatedAt        time.Time
}

// Kind classifies inbox items.
type Kind string

// Inbox item kinds.
const (
	KindPrompt Kind = "prompt"
	KindStop   Kind = "stop"
	KindNew    Kind = "new"
	KindHelp   Kind = "help"
)

// State is the inbox state machine.
type State string

// Inbox states.
const (
	StateQueued    State = "queued"
	StateAdmitting State = "admitting"
	StateAdmitted  State = "admitted"
	StateTerminal  State = "terminal"
	StateRejected  State = "rejected"
	StateCanceled  State = "canceled"
	StatePending   State = "pending" // controls awaiting settlement
	StateComplete  State = "complete"
)

// Result codes stored with terminal inbox items.
const (
	CodeCompleted    = "completed"
	CodeInterrupted  = "interrupted"
	CodeOutputLimit  = "output_limit"
	CodeFailed       = "failed"
	CodeDuplicate    = "duplicate"
	CodeOverflow     = "overflow"
	CodeOversize     = "oversize"
	CodeHistoryLimit = "history_limit"
	CodeConflict     = "admission_conflict"
	CodeBusy         = "busy"
	CodeDenied       = "denied"
	CodeCanceled     = "canceled"
	CodeUnavailable  = "unavailable"
)

// Item is an inbox row.
type Item struct {
	ID                int64
	DedupKey          string
	Platform          Platform
	Installation      string
	MessageID         string
	RouteKey          string
	Generation        int64
	Seq               int64
	Kind              Kind
	Actor             string
	ActorLabel        string
	Content           string
	ContentHash       string
	FilesNotice       bool
	AdmissionKey      string
	State             State
	RunID             string
	ReceiptUserMsg    string
	ReceiptAssistant  string
	RunStatus         string
	ResultCode        string
	StopTargetRunID   string
	StopTargetInboxID int64
	StopCutoffSeq     int64
	Attempts          int
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Stop returns the frozen stop target for a stop control; ok is false for
// any other kind.
func (i Item) Stop() (targetInbox int64, targetRun string, cutoff int64, ok bool) {
	if i.Kind != KindStop {
		return 0, "", 0, false
	}
	return i.StopTargetInboxID, i.StopTargetRunID, i.StopCutoffSeq, true
}

// Receipt returns the admission receipt of an admitted or terminal prompt;
// ok is false before admission or for controls.
func (i Item) Receipt() (runID, userMsg, assistantMsg string, ok bool) {
	if i.Kind != KindPrompt || i.RunID == "" {
		return "", "", "", false
	}
	return i.RunID, i.ReceiptUserMsg, i.ReceiptAssistant, true
}

// Inbound is a normalized platform event ready for ingestion.
type Inbound struct {
	Route       Route
	MessageID   string
	Actor       string
	ActorLabel  string
	Kind        Kind
	Content     string
	FilesNotice bool
	ReceivedAt  time.Time
	// RejectCode, when set, stores the event as a rejected tombstone with
	// this result code instead of admitting it.
	RejectCode string
}

// DedupKey returns the canonical event identity.
func (in Inbound) DedupKey() string {
	return fmt.Sprintf("%s|%s|%s|%s", in.Route.Platform, in.Route.Installation, in.Route.Channel, in.MessageID)
}

// AdmissionKey derives the opaque runtime admission key.
func (in Inbound) AdmissionKey() string {
	sum := sha256.Sum256([]byte(in.DedupKey()))
	return "evt-" + hex.EncodeToString(sum[:])
}

// ContentHash returns the hash of the normalized content.
func ContentHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// Disposition is the result of ingestion.
type Disposition struct {
	Outcome Outcome
	Item    Item
	// Conversation is the route record at commit time.
	Conversation Conversation
	// StopNoop reports that a stop control found nothing running or queued
	// and was completed on arrival.
	StopNoop bool
}

// Outcome classifies ingestion.
type Outcome string

// Ingestion outcomes.
const (
	OutcomeAccepted  Outcome = "accepted"
	OutcomeDuplicate Outcome = "duplicate"
	OutcomeRejected  Outcome = "rejected"
)

// DeliveryStatus is the safe delivery state.
type DeliveryStatus string

// Delivery statuses.
const (
	DeliveryPending   DeliveryStatus = "pending"
	DeliveryAcked     DeliveryStatus = "acked"
	DeliveryAmbiguous DeliveryStatus = "ambiguous_create"
	DeliveryFailed    DeliveryStatus = "failed"
)

// OpState is the delivery operation state.
type OpState string

// Delivery operation states.
const (
	OpPlanned      OpState = "planned"
	OpCreateIntent OpState = "create_intent"
	OpCreated      OpState = "created"
	OpEditPending  OpState = "edit_pending"
	OpAcked        OpState = "acked"
)

// Delivery is a delivery row.
type Delivery struct {
	ID              int64
	RouteKey        string
	Generation      int64
	InboxID         int64
	RunID           string
	DeliverySeq     int64
	ChunkIndex      int
	Destination     Destination
	RemoteID        string
	Nonce           string
	DesiredRevision int64
	AckedRevision   int64
	DesiredText     string
	HasDesiredText  bool
	ContentHash     string
	Status          DeliveryStatus
	Op              OpState
	Attempts        int
	FirstAttemptAt  time.Time
	RetryAt         time.Time
	Audit           string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Resolved reports whether the row needs no further work.
func (d Delivery) Resolved() bool {
	return d.Status == DeliveryAcked && d.AckedRevision == d.DesiredRevision
}
