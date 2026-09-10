package agentbridge

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/mattsp1290/eino-agent/session"
)

// IDs generates collision-resistant durable identifiers. Runs, messages
// and parts must never collide across process restarts.
type IDs struct{}

func random(prefix string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

// NewRunID implements runtime.IDGenerator.
func (IDs) NewRunID() session.RunID { return session.RunID(random("run-")) }

// NewMessageID implements runtime.IDGenerator.
func (IDs) NewMessageID() session.MessageID { return session.MessageID(random("msg-")) }

// NewPartID implements runtime.IDGenerator.
func (IDs) NewPartID() session.PartID { return session.PartID(random("part-")) }

// NewToolCallID implements runtime.IDGenerator.
func (IDs) NewToolCallID() session.ToolCallID { return session.ToolCallID(random("call-")) }

// NewEventID implements runtime.IDGenerator.
func (IDs) NewEventID() session.EventID { return session.EventID(random("evt-")) }

// NewEpochID implements runtime.IDGenerator.
func (IDs) NewEpochID() session.EpochID { return session.EpochID(random("epoch-")) }
