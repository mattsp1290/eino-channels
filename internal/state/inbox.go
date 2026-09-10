package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Capacity bounds ingestion.
type Capacity struct {
	MaxQueuedPerRoute int
	MaxPendingGlobal  int
}

const itemColumns = `id, dedup_key, platform, installation, message_id, route_key, generation, seq, kind, actor, actor_label, COALESCE(content, ''), content_hash, files_notice, admission_key, state, run_id, receipt_user_msg, receipt_assistant_msg, run_status, result_code, stop_target_run_id, stop_target_inbox_id, stop_cutoff_seq, attempts, created_at, updated_at`

func scanItem(row interface{ Scan(...any) error }) (Item, error) {
	var it Item
	var files int
	var created, updated int64
	err := row.Scan(&it.ID, &it.DedupKey, &it.Platform, &it.Installation, &it.MessageID, &it.RouteKey, &it.Generation, &it.Seq, &it.Kind, &it.Actor, &it.ActorLabel, &it.Content, &it.ContentHash, &files, &it.AdmissionKey, &it.State, &it.RunID, &it.ReceiptUserMsg, &it.ReceiptAssistant, &it.RunStatus, &it.ResultCode, &it.StopTargetRunID, &it.StopTargetInboxID, &it.StopCutoffSeq, &it.Attempts, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return Item{}, ErrNotFound
	}
	if err != nil {
		return Item{}, storageErr(err)
	}
	it.FilesNotice = files != 0
	it.CreatedAt = time.Unix(0, created).UTC()
	it.UpdatedAt = time.Unix(0, updated).UTC()
	return it, nil
}

func getItemTx(ctx context.Context, tx *sql.Tx, id int64) (Item, error) {
	return scanItem(tx.QueryRowContext(ctx, `SELECT `+itemColumns+` FROM inbox WHERE id = ?`, id))
}

// GetItem loads one inbox row.
func (s *Store) GetItem(ctx context.Context, id int64) (Item, error) {
	return scanItem(s.host.QueryRowContext(ctx, `SELECT `+itemColumns+` FROM inbox WHERE id = ?`, id))
}

// admitFn applies one kind's admission policy inside the ingest
// transaction: it sets item.State (and any control fields) and may update
// the disposition.
type admitFn func(ctx context.Context, tx *sql.Tx, key string, limits Capacity, now time.Time, item *Item, d *Disposition) error

var admitByKind = map[Kind]admitFn{
	KindPrompt: admitPrompt,
	KindStop:   admitStop,
	KindNew:    admitNew,
	KindHelp:   admitHelp,
}

// Ingest durably records one platform event. Dedup, capacity reservation,
// route creation and control semantics commit in one transaction.
//
// Prompts and controls that exceed capacity or preconditions are stored as
// rejected rows (so a retried event cannot be admitted later) and returned
// with OutcomeRejected and Item.ResultCode set.
func (s *Store) Ingest(ctx context.Context, in Inbound, limits Capacity) (Disposition, error) {
	var d Disposition
	now := in.ReceivedAt
	if now.IsZero() {
		now = time.Now()
	}
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		dedup := in.DedupKey()
		existing, err := scanItem(tx.QueryRowContext(ctx, `SELECT `+itemColumns+` FROM inbox WHERE dedup_key = ?`, dedup))
		if err == nil {
			d = Disposition{Outcome: OutcomeDuplicate, Item: existing}
			d.Conversation, err = getConversationTx(ctx, tx, existing.RouteKey)
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		conv, err := ensureConversationTx(ctx, tx, in.Route, in.Actor, now)
		if err != nil {
			return err
		}
		d.Conversation = conv
		key := in.Route.Key()
		item := Item{DedupKey: dedup, Platform: in.Route.Platform, Installation: in.Route.Installation, MessageID: in.MessageID, RouteKey: key, Generation: conv.Generation, Kind: in.Kind, Actor: in.Actor, ActorLabel: in.ActorLabel, Content: in.Content, ContentHash: ContentHash(in.Content), FilesNotice: in.FilesNotice, AdmissionKey: in.AdmissionKey(), CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
		if in.RejectCode != "" {
			item.State, item.ResultCode = StateRejected, in.RejectCode
			item.Content = ""
		} else {
			admit, ok := admitByKind[in.Kind]
			if !ok {
				return fmt.Errorf("%w: unknown inbox kind", ErrConflict)
			}
			if err := admit(ctx, tx, key, limits, now, &item, &d); err != nil {
				return err
			}
		}
		if item.Seq, err = nextSeqTx(ctx, tx, key); err != nil {
			return err
		}
		if err := insertItemTx(ctx, tx, &item, now); err != nil {
			return err
		}
		d.Item = item
		d.Outcome = OutcomeAccepted
		if item.State == StateRejected {
			d.Outcome = OutcomeRejected
		}
		return nil
	})
	if err != nil {
		return Disposition{}, err
	}
	return d, nil
}

// admitPrompt reserves queue capacity or rejects with an overflow tombstone.
func admitPrompt(ctx context.Context, tx *sql.Tx, key string, limits Capacity, _ time.Time, item *Item, _ *Disposition) error {
	var perRoute, global int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbox WHERE route_key = ? AND state IN (?, ?, ?)`, key, StateQueued, StateAdmitting, StateAdmitted).Scan(&perRoute); err != nil {
		return storageErr(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbox WHERE state IN (?, ?)`, StateQueued, StatePending).Scan(&global); err != nil {
		return storageErr(err)
	}
	// perRoute counts the active item too, so one active plus MaxQueuedPerRoute
	// queued prompts are allowed; the global bound is strict.
	if perRoute > limits.MaxQueuedPerRoute || global >= limits.MaxPendingGlobal {
		item.State, item.ResultCode = StateRejected, CodeOverflow
		item.Content = ""
		return nil
	}
	item.State = StateQueued
	return nil
}

// admitStop freezes the target and cutoff, cancels queued prompts up to the
// cutoff, and completes on arrival when there was nothing to stop.
func admitStop(ctx context.Context, tx *sql.Tx, key string, _ Capacity, now time.Time, item *Item, d *Disposition) error {
	item.State, item.Content = StatePending, ""
	target, err := scanItem(tx.QueryRowContext(ctx, `SELECT `+itemColumns+` FROM inbox WHERE route_key = ? AND state IN (?, ?) ORDER BY seq LIMIT 1`, key, StateAdmitting, StateAdmitted))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil {
		item.StopTargetInboxID = target.ID
		item.StopTargetRunID = target.RunID
	}
	var cutoff sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(seq) FROM inbox WHERE route_key = ?`, key).Scan(&cutoff); err != nil {
		return storageErr(err)
	}
	item.StopCutoffSeq = cutoff.Int64
	res, err := tx.ExecContext(ctx, `UPDATE inbox SET state = ?, result_code = ?, content = NULL, updated_at = ? WHERE route_key = ? AND state = ? AND kind = ? AND seq <= ?`, StateCanceled, CodeCanceled, now.UnixNano(), key, StateQueued, KindPrompt, cutoff.Int64)
	if err != nil {
		return storageErr(err)
	}
	canceled, _ := res.RowsAffected()
	if canceled == 0 && item.StopTargetInboxID == 0 {
		item.State = StateComplete
		d.StopNoop = true
	}
	return nil
}

// admitNew rotates the generation when the route is idle, else rejects busy.
func admitNew(ctx context.Context, tx *sql.Tx, key string, _ Capacity, _ time.Time, item *Item, d *Disposition) error {
	item.Content = ""
	rotated, err := rotateGenerationTx(ctx, tx, key)
	switch {
	case errors.Is(err, ErrConflict):
		item.State, item.ResultCode = StateRejected, CodeBusy
	case err != nil:
		return err
	default:
		item.State = StateComplete
		d.Conversation = rotated
		item.Generation = rotated.Generation
	}
	return nil
}

// admitHelp records the control for dedup only.
func admitHelp(_ context.Context, _ *sql.Tx, _ string, _ Capacity, _ time.Time, item *Item, _ *Disposition) error {
	item.Content, item.State = "", StateComplete
	return nil
}

func insertItemTx(ctx context.Context, tx *sql.Tx, item *Item, now time.Time) error {
	var content any
	if item.Content != "" || item.Kind == KindPrompt && item.State == StateQueued {
		content = item.Content
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO inbox (dedup_key, platform, installation, message_id, route_key, generation, seq, kind, actor, actor_label, content, content_hash, files_notice, admission_key, state, result_code, stop_target_run_id, stop_target_inbox_id, stop_cutoff_seq, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.DedupKey, item.Platform, item.Installation, item.MessageID, item.RouteKey, item.Generation, item.Seq, item.Kind, item.Actor, item.ActorLabel, content, item.ContentHash, boolInt(item.FilesNotice), item.AdmissionKey, item.State, item.ResultCode, item.StopTargetRunID, item.StopTargetInboxID, item.StopCutoffSeq, now.UnixNano(), now.UnixNano())
	if err != nil {
		return storageErr(err)
	}
	item.ID, _ = res.LastInsertId()
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// NextWork returns the next item for the route: a pending control first,
// otherwise the lowest-sequence queued prompt. ErrNotFound means idle.
func (s *Store) NextWork(ctx context.Context, routeKey string) (Item, error) {
	it, err := scanItem(s.host.QueryRowContext(ctx, `SELECT `+itemColumns+` FROM inbox WHERE route_key = ? AND state = ? ORDER BY seq LIMIT 1`, routeKey, StatePending))
	if err == nil {
		return it, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Item{}, err
	}
	return scanItem(s.host.QueryRowContext(ctx, `SELECT `+itemColumns+` FROM inbox WHERE route_key = ? AND state IN (?, ?, ?) ORDER BY seq LIMIT 1`, routeKey, StateAdmitting, StateAdmitted, StateQueued))
}

// RoutesWithWork lists route keys greater than after that have runnable or
// recoverable inbox work or unresolved deliveries, bounded by limit. The
// scheduler rotates the cursor so no route starves behind lexicographically
// smaller ones.
func (s *Store) RoutesWithWork(ctx context.Context, after string, limit int) ([]string, error) {
	rows, err := s.host.QueryContext(ctx, `SELECT route_key FROM (
		SELECT route_key FROM inbox WHERE state IN (?, ?, ?, ?)
		UNION SELECT route_key FROM deliveries WHERE `+schedulableDelivery+`
	) WHERE route_key > ? ORDER BY route_key LIMIT ?`, StateQueued, StateAdmitting, StateAdmitted, StatePending, after, limit)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, storageErr(err)
		}
		keys = append(keys, k)
	}
	return keys, storageErr(rows.Err())
}

// Transition applies a guarded state change. from may be empty to skip the
// guard. Content is cleared when the new state is terminal-ish.
func (s *Store) Transition(ctx context.Context, id int64, from, to State, code string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error { return transitionTx(ctx, tx, id, from, to, code) })
}

func transitionTx(ctx context.Context, tx *sql.Tx, id int64, from, to State, code string) error {
	clear := ""
	switch to {
	case StateTerminal, StateRejected, StateCanceled, StateComplete:
		clear = ", content = NULL"
	}
	q := `UPDATE inbox SET state = ?, result_code = CASE WHEN ? = '' THEN result_code ELSE ? END, updated_at = ?` + clear + ` WHERE id = ?`
	args := []any{to, code, code, time.Now().UnixNano(), id}
	if from != "" {
		q += ` AND state = ?`
		args = append(args, from)
	}
	res, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		return storageErr(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: inbox %d not in state %q", ErrConflict, id, from)
	}
	return nil
}

// MarkAdmitted records the runtime receipt and moves the item to admitted.
// The prompt payload is retained until the run is terminal so a crash before
// receipt persistence can still replay the frozen request.
func (s *Store) MarkAdmitted(ctx context.Context, id int64, runID, userMsg, assistantMsg string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE inbox SET state = ?, run_id = ?, receipt_user_msg = ?, receipt_assistant_msg = ?, updated_at = ? WHERE id = ? AND state IN (?, ?)`, StateAdmitted, runID, userMsg, assistantMsg, time.Now().UnixNano(), id, StateAdmitting, StateAdmitted)
		if err != nil {
			return storageErr(err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: inbox %d not admitting", ErrConflict, id)
		}
		return nil
	})
}

// IncrementAttempts bumps the admission attempt counter.
func (s *Store) IncrementAttempts(ctx context.Context, id int64) (int, error) {
	var n int
	err := s.host.QueryRowContext(ctx, `UPDATE inbox SET attempts = attempts + 1, updated_at = ? WHERE id = ? RETURNING attempts`, time.Now().UnixNano(), id).Scan(&n)
	return n, storageErr(err)
}

// MarkTerminal records the run status, clears the prompt payload and plans
// the deliveries for the run's committed output in the same transaction.
func (s *Store) MarkTerminal(ctx context.Context, id int64, runStatus, code string, plans []DeliveryPlan) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		item, err := getItemTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if item.State == StateTerminal {
			return nil
		}
		if item.State != StateAdmitted || item.RunID == "" {
			return fmt.Errorf("%w: inbox %d is %s without a run", ErrConflict, id, item.State)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE inbox SET state = ?, run_status = ?, result_code = ?, content = NULL, updated_at = ? WHERE id = ?`, StateTerminal, runStatus, code, time.Now().UnixNano(), id); err != nil {
			return storageErr(err)
		}
		return planDeliveriesTx(ctx, tx, item, plans)
	})
}

// PendingStops lists unsettled stop controls for a route.
func (s *Store) PendingStops(ctx context.Context, routeKey string) ([]Item, error) {
	return s.listItems(ctx, `SELECT `+itemColumns+` FROM inbox WHERE route_key = ? AND kind = ? AND state = ? ORDER BY seq`, routeKey, KindStop, StatePending)
}

// RecoveryItems lists prompt items that were admitting or admitted when the
// process last stopped, bounded by limit. It is a diagnostic helper: the
// scheduler recovers through RoutesWithWork and NextWork.
func (s *Store) RecoveryItems(ctx context.Context, limit int) ([]Item, error) {
	return s.listItems(ctx, `SELECT `+itemColumns+` FROM inbox WHERE kind = ? AND state IN (?, ?) ORDER BY id LIMIT ?`, KindPrompt, StateAdmitting, StateAdmitted, limit)
}

// ActiveItem returns the admitting/admitted prompt of a route.
func (s *Store) ActiveItem(ctx context.Context, routeKey string) (Item, error) {
	return scanItem(s.host.QueryRowContext(ctx, `SELECT `+itemColumns+` FROM inbox WHERE route_key = ? AND kind = ? AND state IN (?, ?) ORDER BY seq LIMIT 1`, routeKey, KindPrompt, StateAdmitting, StateAdmitted))
}

// CancelQueuedUpTo marks queued prompts at or below cutoff canceled.
func (s *Store) CancelQueuedUpTo(ctx context.Context, routeKey string, cutoff int64) error {
	_, err := s.host.ExecContext(ctx, `UPDATE inbox SET state = ?, result_code = ?, content = NULL, updated_at = ? WHERE route_key = ? AND state = ? AND kind = ? AND seq <= ?`, StateCanceled, CodeCanceled, time.Now().UnixNano(), routeKey, StateQueued, KindPrompt, cutoff)
	return storageErr(err)
}

// CountByState counts inbox rows per state (for diagnostics and tests).
func (s *Store) CountByState(ctx context.Context, state State) (int, error) {
	var n int
	err := s.host.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbox WHERE state = ?`, state).Scan(&n)
	return n, storageErr(err)
}

func (s *Store) listItems(ctx context.Context, query string, args ...any) ([]Item, error) {
	rows, err := s.host.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	var items []Item
	for rows.Next() {
		it, err := scanItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, storageErr(rows.Err())
}
