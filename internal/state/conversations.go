package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

func newRuntimeSessionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "sess-" + hex.EncodeToString(b[:])
}

func scanConversation(row interface{ Scan(...any) error }) (Conversation, error) {
	var c Conversation
	var created int64
	err := row.Scan(&c.Route.Platform, &c.Route.Installation, &c.Route.Channel, &c.Route.ThreadRoot, &c.Route.DMActor, &c.Generation, &c.RuntimeSessionID, &c.CreatorActor, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return Conversation{}, ErrNotFound
	}
	if err != nil {
		return Conversation{}, storageErr(err)
	}
	c.CreatedAt = time.Unix(0, created).UTC()
	return c, nil
}

const conversationColumns = `platform, installation, channel, thread_root, dm_actor, generation, runtime_session_id, creator_actor, created_at`

func getConversationTx(ctx context.Context, tx *sql.Tx, key string) (Conversation, error) {
	return scanConversation(tx.QueryRowContext(ctx, `SELECT `+conversationColumns+` FROM conversations WHERE route_key = ?`, key))
}

// GetConversation loads a route.
func (s *Store) GetConversation(ctx context.Context, route Route) (Conversation, error) {
	return scanConversation(s.host.QueryRowContext(ctx, `SELECT `+conversationColumns+` FROM conversations WHERE route_key = ?`, route.Key()))
}

// GetConversationByKey loads a route by its canonical key.
func (s *Store) GetConversationByKey(ctx context.Context, key string) (Conversation, error) {
	return scanConversation(s.host.QueryRowContext(ctx, `SELECT `+conversationColumns+` FROM conversations WHERE route_key = ?`, key))
}

func ensureConversationTx(ctx context.Context, tx *sql.Tx, route Route, creator string, now time.Time) (Conversation, error) {
	c, err := getConversationTx(ctx, tx, route.Key())
	if err == nil {
		return c, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Conversation{}, err
	}
	c = Conversation{Route: route, Generation: 1, RuntimeSessionID: newRuntimeSessionID(), CreatorActor: creator, CreatedAt: now.UTC()}
	_, err = tx.ExecContext(ctx, `INSERT INTO conversations (route_key, generation, runtime_session_id, platform, installation, channel, thread_root, dm_actor, creator_actor, created_at, next_seq) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, 1)`,
		route.Key(), c.RuntimeSessionID, route.Platform, route.Installation, route.Channel, route.ThreadRoot, route.DMActor, creator, now.UnixNano())
	if err != nil {
		return Conversation{}, storageErr(err)
	}
	return c, nil
}

func nextSeqTx(ctx context.Context, tx *sql.Tx, key string) (int64, error) {
	var seq int64
	if err := tx.QueryRowContext(ctx, `UPDATE conversations SET next_seq = next_seq + 1 WHERE route_key = ? RETURNING next_seq - 1`, key).Scan(&seq); err != nil {
		return 0, storageErr(err)
	}
	return seq, nil
}

// BindThread durably maps a platform root message to a created thread. It is
// idempotent: a second call returns the existing binding.
func (s *Store) BindThread(ctx context.Context, platform Platform, installation, sourceMessageID, threadID string) (string, error) {
	var bound string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT thread_id FROM thread_bindings WHERE platform = ? AND installation = ? AND source_message_id = ?`, platform, installation, sourceMessageID).Scan(&bound)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return storageErr(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO thread_bindings (platform, installation, source_message_id, thread_id, created_at) VALUES (?, ?, ?, ?, ?)`, platform, installation, sourceMessageID, threadID, time.Now().UnixNano()); err != nil {
			return storageErr(err)
		}
		bound = threadID
		return nil
	})
	return bound, err
}

// LookupThread returns the bound thread for a root message.
func (s *Store) LookupThread(ctx context.Context, platform Platform, installation, sourceMessageID string) (string, error) {
	var bound string
	err := s.host.QueryRowContext(ctx, `SELECT thread_id FROM thread_bindings WHERE platform = ? AND installation = ? AND source_message_id = ?`, platform, installation, sourceMessageID).Scan(&bound)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return bound, storageErr(err)
}

// routeBusyTx reports whether the route has unfinished prompt work, pending
// controls, or unresolved deliveries.
func routeBusyTx(ctx context.Context, tx *sql.Tx, key string) (bool, error) {
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbox WHERE route_key = ? AND state IN (?, ?, ?, ?)`, key, StateQueued, StateAdmitting, StateAdmitted, StatePending).Scan(&n); err != nil {
		return false, storageErr(err)
	}
	if n != 0 {
		return true, nil
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE route_key = ? AND NOT (status = ? AND acked_revision = desired_revision)`, key, DeliveryAcked).Scan(&n); err != nil {
		return false, storageErr(err)
	}
	return n != 0, nil
}

func rotateGenerationTx(ctx context.Context, tx *sql.Tx, key string) (Conversation, error) {
	busy, err := routeBusyTx(ctx, tx, key)
	if err != nil {
		return Conversation{}, err
	}
	if busy {
		return Conversation{}, fmt.Errorf("%w: route has unfinished work", ErrConflict)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE conversations SET generation = generation + 1, runtime_session_id = ? WHERE route_key = ?`, newRuntimeSessionID(), key); err != nil {
		return Conversation{}, storageErr(err)
	}
	return getConversationTx(ctx, tx, key)
}
