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

const deliveryColumns = `d.id, d.route_key, d.generation, d.inbox_id, d.run_id, d.delivery_seq, d.chunk_index, d.platform, d.installation, d.channel, d.thread_root, c.dm_actor, d.remote_id, d.nonce, d.desired_revision, d.acked_revision, d.desired_text, d.content_hash, d.status, d.op_state, d.attempts, d.first_attempt_at, d.retry_at, d.audit, d.created_at, d.updated_at`

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
	conv, err := getConversationTx(ctx, tx, item.RouteKey)
	if err != nil {
		return err
	}
	dest := conv.Route.Destination()
	now := time.Now().UnixNano()
	for _, p := range plans {
		res, err := tx.ExecContext(ctx, `UPDATE deliveries SET desired_revision = ?, desired_text = ?, content_hash = ?, op_state = CASE WHEN remote_id = '' THEN op_state ELSE ? END, updated_at = ? WHERE run_id = ? AND chunk_index = ?`, p.Revision, p.Text, ContentHash(p.Text), OpEditPending, now, item.RunID, p.ChunkIndex)
		if err != nil {
			return storageErr(err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			continue
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
		out, err = scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d JOIN conversations c ON c.route_key = d.route_key WHERE d.run_id = ? AND d.chunk_index = 0`, item.RunID))
		return err
	})
	return out, err
}

// NextDelivery returns the lowest-ordered unresolved delivery of the route
// and whether the lane is blocked by a failed or ambiguous predecessor.
func (s *Store) NextDelivery(ctx context.Context, routeKey string) (Delivery, bool, error) {
	d, err := scanDelivery(s.host.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d JOIN conversations c ON c.route_key = d.route_key WHERE d.route_key = ? AND NOT (d.status = ? AND d.acked_revision = d.desired_revision) ORDER BY d.delivery_seq, d.chunk_index LIMIT 1`, routeKey, DeliveryAcked))
	if err != nil {
		return Delivery{}, false, err
	}
	blocked := d.Status == DeliveryFailed || d.Status == DeliveryAmbiguous
	return d, blocked, nil
}

// GetDelivery loads one row.
func (s *Store) GetDelivery(ctx context.Context, id int64) (Delivery, error) {
	return scanDelivery(s.host.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d JOIN conversations c ON c.route_key = d.route_key WHERE d.id = ?`, id))
}

// DeliveryForRun loads the chunk row of a run.
func (s *Store) DeliveryForRun(ctx context.Context, runID string, chunk int) (Delivery, error) {
	return scanDelivery(s.host.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d JOIN conversations c ON c.route_key = d.route_key WHERE d.run_id = ? AND d.chunk_index = ?`, runID, chunk))
}

// MarkCreateIntent persists the intent (and nonce) before a create call.
func (s *Store) MarkCreateIntent(ctx context.Context, id int64) (Delivery, error) {
	var out Delivery
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		now := time.Now().UnixNano()
		if _, err := tx.ExecContext(ctx, `UPDATE deliveries SET op_state = ?, nonce = CASE WHEN nonce = '' THEN ? ELSE nonce END, attempts = attempts + 1, first_attempt_at = CASE WHEN first_attempt_at = 0 THEN ? ELSE first_attempt_at END, updated_at = ? WHERE id = ? AND remote_id = ''`, OpCreateIntent, newNonce(), now, now, id); err != nil {
			return storageErr(err)
		}
		var err error
		out, err = scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d JOIN conversations c ON c.route_key = d.route_key WHERE d.id = ?`, id))
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

// MarkAttempt records a retry schedule after a definite failure.
func (s *Store) MarkAttempt(ctx context.Context, id int64, retryAt time.Time) error {
	now := time.Now().UnixNano()
	_, err := s.host.ExecContext(ctx, `UPDATE deliveries SET attempts = attempts + 1, first_attempt_at = CASE WHEN first_attempt_at = 0 THEN ? ELSE first_attempt_at END, retry_at = ?, updated_at = ? WHERE id = ?`, now, retryAt.UnixNano(), now, id)
	return storageErr(err)
}

// MarkAmbiguous records an unknown create outcome.
func (s *Store) MarkAmbiguous(ctx context.Context, id int64) error {
	_, err := s.host.ExecContext(ctx, `UPDATE deliveries SET status = ?, updated_at = ? WHERE id = ? AND remote_id = ''`, DeliveryAmbiguous, time.Now().UnixNano(), id)
	return storageErr(err)
}

// MarkFailed records a permanent delivery failure for operator resolution.
func (s *Store) MarkFailed(ctx context.Context, id int64, audit string) error {
	_, err := s.host.ExecContext(ctx, `UPDATE deliveries SET status = ?, audit = ?, updated_at = ? WHERE id = ?`, DeliveryFailed, audit, time.Now().UnixNano(), id)
	return storageErr(err)
}

// SetDesired updates the desired text/revision of an existing row (used
// when a run settles after its preview row exists).
func (s *Store) SetDesired(ctx context.Context, id int64, text string, revision int64) error {
	_, err := s.host.ExecContext(ctx, `UPDATE deliveries SET desired_revision = ?, desired_text = ?, content_hash = ?, op_state = CASE WHEN remote_id = '' THEN op_state ELSE ? END, status = CASE WHEN status = ? THEN ? ELSE status END, updated_at = ? WHERE id = ?`, revision, text, ContentHash(text), OpEditPending, DeliveryAcked, DeliveryPending, time.Now().UnixNano(), id)
	return storageErr(err)
}

// ListDeliveries returns bounded rows for operator listing.
func (s *Store) ListDeliveries(ctx context.Context, limit int) ([]Delivery, error) {
	rows, err := s.host.QueryContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d JOIN conversations c ON c.route_key = d.route_key WHERE NOT (d.status = ? AND d.acked_revision = d.desired_revision) ORDER BY d.id LIMIT ?`, DeliveryAcked, limit)
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
		d, err := scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d JOIN conversations c ON c.route_key = d.route_key WHERE d.id = ?`, id))
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
		d, err := scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM deliveries d JOIN conversations c ON c.route_key = d.route_key WHERE d.id = ?`, id))
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
	err := s.host.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE route_key = ? AND NOT (status = ? AND acked_revision = desired_revision)`, routeKey, DeliveryAcked).Scan(&n)
	return n, storageErr(err)
}
