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

// DeliveryPlan describes one chunk to deliver for a run.
type DeliveryPlan struct {
	ChunkIndex int
	Text       string
	// Revision is the desired final content revision. Revision 0 is the
	// transient status placeholder; the first committed final text is 1.
	Revision int64
}

func newNonce() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:]) // 24 characters, within Discord's 25-character nonce limit
}

// unresolvedDelivery is the single definition of "this row still needs
// work": every scan, count and lane query must use it so they cannot drift.
// unresolvedDeliveryD is the same predicate qualified with the `d` alias for
// joined queries; TestPredicatesAgree keeps the two in step.
const (
	unresolvedDelivery  = `NOT (status = 'acked' AND acked_revision = desired_revision)`
	unresolvedDeliveryD = `NOT (d.status = 'acked' AND d.acked_revision = d.desired_revision)`
)

// schedulableDelivery narrows unresolvedDelivery to rows automation can
// still advance; failed and ambiguous rows wait for operator resolution and
// must not re-run their route on every scan.
const schedulableDelivery = `status = 'pending'`

// Delivery reads LEFT JOIN conversations for dm_actor so an orphaned row can
// never silently vanish from the lane.
const deliveryColumns = `d.id, d.route_key, d.generation, d.inbox_id, d.run_id, d.delivery_seq, d.chunk_index, d.platform, d.installation, d.channel, d.thread_root, COALESCE(c.dm_actor, ''), d.remote_id, d.nonce, d.desired_revision, d.acked_revision, d.desired_text, d.content_hash, d.status, d.op_state, d.attempts, d.first_attempt_at, d.retry_at, d.audit, d.created_at, d.updated_at`

func scanDelivery(row interface{ Scan(...any) error }) (Delivery, error) {
	var d Delivery
	var text sql.NullString
	var dmActor string
	var first, retry, created, updated int64
	err := row.Scan(&d.ID, &d.RouteKey, &d.Generation, &d.InboxID, &d.RunID, &d.DeliverySeq, &d.ChunkIndex, &d.Destination.Platform, &d.Destination.Installation, &d.Destination.Channel, &d.Destination.ThreadRoot, &dmActor, &d.RemoteID, &d.Nonce, &d.DesiredRevision, &d.AckedRevision, &text, &d.ContentHash, &d.Status, &d.Op, &d.Attempts, &first, &retry, &d.Audit, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return Delivery{}, ErrNotFound
	}
	if err != nil {
		return Delivery{}, storageErr(err)
	}
	d.DesiredText, d.HasDesiredText = text.String, text.Valid
	d.Destination.DMActor = dmActor
	if first != 0 {
		d.FirstAttemptAt = time.Unix(0, first).UTC()
	}
	if retry != 0 {
		d.RetryAt = time.Unix(0, retry).UTC()
	}
	d.CreatedAt = time.Unix(0, created).UTC()
	d.UpdatedAt = time.Unix(0, updated).UTC()
	return d, nil
}

// planDeliveriesTx inserts rows for a terminal item. Chunk 0 may already
// exist as the preview/status row; it is updated to the final revision.
func planDeliveriesTx(ctx context.Context, tx *sql.Tx, item Item, plans []DeliveryPlan) error {
	if item.RunID == "" {
		return fmt.Errorf("%w: deliveries require a run ID", ErrConflict)
	}
	conv, err := getConversationTx(ctx, tx, item.RouteKey)
	if err != nil {
		return err
	}
	dest := conv.Route.Destination()
	now := time.Now().UnixNano()
	for _, p := range plans {
		// Revisions are monotonic: a planned revision never lowers a row.
		// A row already acknowledged at a lower revision returns to pending.
		res, err := tx.ExecContext(ctx, `UPDATE deliveries SET desired_revision = ?, desired_text = ?, content_hash = ?, op_state = CASE WHEN remote_id = '' THEN op_state ELSE ? END, status = CASE WHEN status = ? AND acked_revision <> ? THEN ? ELSE status END, updated_at = ? WHERE run_id = ? AND chunk_index = ? AND desired_revision <= ?`, p.Revision, p.Text, ContentHash(p.Text), OpEditPending, DeliveryAcked, p.Revision, DeliveryPending, now, item.RunID, p.ChunkIndex, p.Revision)
		if err != nil {
			return storageErr(err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			continue
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE run_id = ? AND chunk_index = ?`, item.RunID, p.ChunkIndex).Scan(&exists); err != nil {
			return storageErr(err)
		}
		if exists != 0 {
			continue // an equal or newer revision is already planned
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO deliveries (route_key, generation, inbox_id, run_id, delivery_seq, chunk_index, platform, installation, channel, thread_root, desired_revision, desired_text, content_hash, status, op_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			item.RouteKey, item.Generation, item.ID, item.RunID, item.Seq, p.ChunkIndex, dest.Platform, dest.Installation, dest.Channel, dest.ThreadRoot, p.Revision, p.Text, ContentHash(p.Text), DeliveryPending, OpPlanned, now, now); err != nil {
			return storageErr(err)
		}
	}
	return nil
}

// PlanPreview inserts the chunk-0 status row for an admitted run before any
// create is attempted. Revision 0 carries the transient placeholder text.
func (s *Store) PlanPreview(ctx context.Context, item Item, placeholder string) (Delivery, error) {
	var out Delivery
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := planDeliveriesTx(ctx, tx, item, []DeliveryPlan{{ChunkIndex: 0, Text: placeholder, Revision: 0}}); err != nil {
			return err
		}
		var err error
		out, err = scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d LEFT JOIN conversations c ON c.route_key = d.route_key WHERE d.run_id = ? AND d.chunk_index = 0`, item.RunID))
		return err
	})
	return out, err
}

// NextDelivery returns the lowest-ordered unresolved delivery of the route
// and whether the lane is blocked by a failed or ambiguous predecessor.
func (s *Store) NextDelivery(ctx context.Context, routeKey string) (Delivery, bool, error) {
	d, err := scanDelivery(s.host.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d LEFT JOIN conversations c ON c.route_key = d.route_key WHERE d.route_key = ? AND `+unresolvedDeliveryD+` ORDER BY d.delivery_seq, d.chunk_index LIMIT 1`, routeKey))
	if err != nil {
		return Delivery{}, false, err
	}
	blocked := d.Status == DeliveryFailed || d.Status == DeliveryAmbiguous
	return d, blocked, nil
}

// GetDelivery loads one row.
func (s *Store) GetDelivery(ctx context.Context, id int64) (Delivery, error) {
	return scanDelivery(s.host.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d LEFT JOIN conversations c ON c.route_key = d.route_key WHERE d.id = ?`, id))
}

// DeliveryForRun loads the chunk row of a run.
func (s *Store) DeliveryForRun(ctx context.Context, runID string, chunk int) (Delivery, error) {
	return scanDelivery(s.host.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d LEFT JOIN conversations c ON c.route_key = d.route_key WHERE d.run_id = ? AND d.chunk_index = ?`, runID, chunk))
}

// MarkCreateIntent persists the intent (and nonce) before a create call.
// It refuses rows that already have a remote ID: once a create returned an
// ID, no path may create again for that row.
func (s *Store) MarkCreateIntent(ctx context.Context, id int64) (Delivery, error) {
	var out Delivery
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		now := time.Now().UnixNano()
		res, err := tx.ExecContext(ctx, `UPDATE deliveries SET op_state = ?, nonce = CASE WHEN nonce = '' THEN ? ELSE nonce END, attempts = attempts + 1, first_attempt_at = CASE WHEN first_attempt_at = 0 THEN ? ELSE first_attempt_at END, updated_at = ? WHERE id = ? AND remote_id = ''`, OpCreateIntent, newNonce(), now, now, id)
		if err != nil {
			return storageErr(err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: delivery %d already has a remote message", ErrConflict, id)
		}
		out, err = scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d LEFT JOIN conversations c ON c.route_key = d.route_key WHERE d.id = ?`, id))
		return err
	})
	return out, err
}

// MarkCreated records the remote message ID and the revision the create
// acknowledged. If the acknowledged revision equals the desired one, the
// payload is cleared.
func (s *Store) MarkCreated(ctx context.Context, id int64, remoteID string, ackedRevision int64) error {
	return s.markAcked(ctx, id, remoteID, ackedRevision, OpCreated)
}

// MarkEdited records an acknowledged edit revision.
func (s *Store) MarkEdited(ctx context.Context, id int64, ackedRevision int64) error {
	return s.markAcked(ctx, id, "", ackedRevision, OpAcked)
}

func (s *Store) markAcked(ctx context.Context, id int64, remoteID string, acked int64, op OpState) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		now := time.Now().UnixNano()
		q := `UPDATE deliveries SET acked_revision = ?, status = ?, attempts = 0, retry_at = 0, first_attempt_at = 0, op_state = CASE WHEN ? = desired_revision THEN ? ELSE ? END, desired_text = CASE WHEN ? = desired_revision THEN NULL ELSE desired_text END, updated_at = ?`
		args := []any{acked, DeliveryAcked, acked, OpAcked, op, acked, now}
		if remoteID != "" {
			q += `, remote_id = ?`
			args = append(args, remoteID)
		}
		q += ` WHERE id = ?`
		args = append(args, id)
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return storageErr(err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrNotFound
		}
		// A row acknowledged below its desired revision stays pending for an edit.
		if _, err := tx.ExecContext(ctx, `UPDATE deliveries SET status = ?, op_state = ? WHERE id = ? AND acked_revision <> desired_revision`, DeliveryPending, OpEditPending, id); err != nil {
			return storageErr(err)
		}
		return nil
	})
}

// MarkAttempt records a failed attempt and its retry schedule. A row
// without a remote ID returns to the planned operation state so that a
// create intent left behind by a crash can be told apart from a definite
// failure. Use it for edits; creates already count their attempt in
// MarkCreateIntent and use MarkRetryAt.
func (s *Store) MarkAttempt(ctx context.Context, id int64, retryAt time.Time) error {
	now := time.Now().UnixNano()
	_, err := s.host.ExecContext(ctx, `UPDATE deliveries SET attempts = attempts + 1, first_attempt_at = CASE WHEN first_attempt_at = 0 THEN ? ELSE first_attempt_at END, retry_at = ?, op_state = CASE WHEN remote_id = '' THEN ? ELSE op_state END, updated_at = ? WHERE id = ?`, now, retryAt.UnixNano(), OpPlanned, now, id)
	return storageErr(err)
}

// MarkRetryAt schedules a retry without counting another attempt; the
// create intent already counted it.
func (s *Store) MarkRetryAt(ctx context.Context, id int64, retryAt time.Time) error {
	now := time.Now().UnixNano()
	_, err := s.host.ExecContext(ctx, `UPDATE deliveries SET retry_at = ?, op_state = CASE WHEN remote_id = '' THEN ? ELSE op_state END, updated_at = ? WHERE id = ?`, retryAt.UnixNano(), OpPlanned, now, id)
	return storageErr(err)
}

// MarkAmbiguous records an unknown create outcome. It refuses rows that
// already have a remote ID.
func (s *Store) MarkAmbiguous(ctx context.Context, id int64) error {
	res, err := s.host.ExecContext(ctx, `UPDATE deliveries SET status = ?, updated_at = ? WHERE id = ? AND remote_id = ''`, DeliveryAmbiguous, time.Now().UnixNano(), id)
	if err != nil {
		return storageErr(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: delivery %d has a remote message", ErrConflict, id)
	}
	return nil
}

// MarkFailed records a permanent delivery failure for operator resolution.
// A row already acknowledged at its desired revision never moves backwards.
func (s *Store) MarkFailed(ctx context.Context, id int64, audit string) error {
	res, err := s.host.ExecContext(ctx, `UPDATE deliveries SET status = ?, audit = ?, updated_at = ? WHERE id = ? AND `+unresolvedDelivery, DeliveryFailed, audit, time.Now().UnixNano(), id)
	if err != nil {
		return storageErr(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: delivery %d is already resolved", ErrConflict, id)
	}
	return nil
}

// SetDesired updates the desired text/revision of an existing row. It is a
// diagnostic/test helper; production planning goes through MarkTerminal.
func (s *Store) SetDesired(ctx context.Context, id int64, text string, revision int64) error {
	_, err := s.host.ExecContext(ctx, `UPDATE deliveries SET desired_revision = ?, desired_text = ?, content_hash = ?, op_state = CASE WHEN remote_id = '' THEN op_state ELSE ? END, status = CASE WHEN status = ? THEN ? ELSE status END, updated_at = ? WHERE id = ?`, revision, text, ContentHash(text), OpEditPending, DeliveryAcked, DeliveryPending, time.Now().UnixNano(), id)
	return storageErr(err)
}

// ListDeliveries returns bounded rows for operator listing.
func (s *Store) ListDeliveries(ctx context.Context, limit int) ([]Delivery, error) {
	rows, err := s.host.QueryContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d LEFT JOIN conversations c ON c.route_key = d.route_key WHERE `+unresolvedDeliveryD+` ORDER BY d.id LIMIT ?`, limit)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	var out []Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, storageErr(rows.Err())
}

// AssociateMessage records an operator-verified remote message ID for an
// ambiguous or failed delivery. It never marks the answer delivered; the
// latest desired revision remains pending as an edit.
func (s *Store) AssociateMessage(ctx context.Context, id int64, remoteID, audit string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		d, err := scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d LEFT JOIN conversations c ON c.route_key = d.route_key WHERE d.id = ?`, id))
		if err != nil {
			return err
		}
		if d.Status != DeliveryAmbiguous && d.Status != DeliveryFailed {
			return fmt.Errorf("%w: delivery %d is %s", ErrConflict, id, d.Status)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE deliveries SET remote_id = ?, status = ?, op_state = ?, attempts = 0, retry_at = 0, first_attempt_at = 0, audit = ?, updated_at = ? WHERE id = ?`, remoteID, DeliveryPending, OpEditPending, audit, time.Now().UnixNano(), id); err != nil {
			return storageErr(err)
		}
		return nil
	})
}

// Resend moves an ambiguous or failed delivery back to pending with an audit
// marker. A duplicate external message is possible and documented.
func (s *Store) Resend(ctx context.Context, id int64, audit string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		d, err := scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d LEFT JOIN conversations c ON c.route_key = d.route_key WHERE d.id = ?`, id))
		if err != nil {
			return err
		}
		if d.Status != DeliveryAmbiguous && d.Status != DeliveryFailed {
			return fmt.Errorf("%w: delivery %d is %s", ErrConflict, id, d.Status)
		}
		op := OpPlanned
		if d.RemoteID != "" {
			op = OpEditPending
		}
		if _, err := tx.ExecContext(ctx, `UPDATE deliveries SET status = ?, op_state = ?, nonce = '', attempts = 0, retry_at = 0, first_attempt_at = 0, audit = ?, updated_at = ? WHERE id = ?`, DeliveryPending, op, audit, time.Now().UnixNano(), id); err != nil {
			return storageErr(err)
		}
		return nil
	})
}

// UnresolvedDeliveries counts rows that are not acknowledged at their desired revision.
func (s *Store) UnresolvedDeliveries(ctx context.Context, routeKey string) (int, error) {
	var n int
	err := s.host.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE route_key = ? AND `+unresolvedDelivery, routeKey).Scan(&n)
	return n, storageErr(err)
}
