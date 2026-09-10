// Package state owns the host-side durable state of eino-channels: the
// exclusive state directory lock, the host database (routing, inbox and
// deliveries) and the host-owned pool for the eino-agent session database.
//
// The host database never contains credentials, prompt bodies after receipt
// confirmation, or final answer text after acknowledged delivery.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	agentsqlite "github.com/mattsp1290/eino-agent/store/sqlite"
	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// File names inside the state directory.
const (
	LockFile      = "lock"
	HostDBFile    = "channels.db"
	AgentDBFile   = "sessions.db"
	dirPerm       = 0o700
	filePerm      = 0o600
	hostSchemaVer = 1
)

var (
	// ErrLocked means another process holds the state directory.
	ErrLocked = errors.New("state directory is locked by another process")
	// ErrProtected means the state directory or a file failed a safety check.
	ErrProtected = errors.New("state directory failed protection checks")
	// ErrSchema means a database has an unknown, newer, or rejected schema.
	ErrSchema = errors.New("database schema is not supported by this binary")
	// ErrStorage marks a storage failure with a fixed safe diagnostic.
	ErrStorage = errors.New("storage failure")
	// ErrNotFound marks a missing record.
	ErrNotFound = errors.New("record not found")
	// ErrConflict marks a precondition failure inside a transaction.
	ErrConflict = errors.New("state conflict")
)

// Store is the opened host state.
type Store struct {
	dir     string
	lock    *os.File
	host    *sql.DB
	agentDB *sql.DB
	agent   *agentsqlite.Store
}

// Options tunes Open.
type Options struct {
	// AgentDB controls whether the eino-agent session database is opened.
	// Operator commands that only inspect deliveries may leave it closed.
	SkipAgentDB bool
}

// Open acquires the exclusive lock, protects the directory, opens and
// migrates the host database and opens the agent session pool.
func Open(ctx context.Context, dir string, opts Options) (*Store, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return nil, fmt.Errorf("%w: state directory must be an absolute clean path", ErrProtected)
	}
	if err := prepareDir(dir); err != nil {
		return nil, err
	}
	st := &Store{dir: dir}
	ok := false
	defer func() {
		if !ok {
			_ = st.Close()
		}
	}()
	var err error
	if st.lock, err = acquireLock(filepath.Join(dir, LockFile)); err != nil {
		return nil, err
	}
	hostPath := filepath.Join(dir, HostDBFile)
	if err = rejectSymlink(hostPath); err != nil {
		return nil, err
	}
	if st.host, err = openPool(hostPath); err != nil {
		return nil, err
	}
	if err = migrateHost(ctx, st.host); err != nil {
		return nil, err
	}
	if err = protectDBFiles(hostPath); err != nil {
		return nil, err
	}
	if !opts.SkipAgentDB {
		agentPath := filepath.Join(dir, AgentDBFile)
		if err = rejectSymlink(agentPath); err != nil {
			return nil, err
		}
		if st.agentDB, err = openPool(agentPath); err != nil {
			return nil, err
		}
		if err = agentsqlite.Migrate(ctx, st.agentDB); err != nil {
			return nil, fmt.Errorf("%w: agent session database rejected (keep the file; use a fresh state directory)", ErrSchema)
		}
		if st.agent, err = agentsqlite.New(ctx, st.agentDB); err != nil {
			return nil, fmt.Errorf("%w: agent session database could not be opened", ErrSchema)
		}
		if err = protectDBFiles(agentPath); err != nil {
			return nil, err
		}
	}
	ok = true
	return st, nil
}

// Dir returns the state directory.
func (s *Store) Dir() string { return s.dir }

// Agent returns the eino-agent store, or nil when SkipAgentDB was set.
func (s *Store) Agent() *agentsqlite.Store { return s.agent }

// Close releases pools and the lock. Callers must finish workers and watch
// subscriptions first.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	if s.agentDB != nil {
		errs = append(errs, s.agentDB.Close())
		s.agentDB = nil
		s.agent = nil
	}
	if s.host != nil {
		errs = append(errs, s.host.Close())
		s.host = nil
	}
	if s.lock != nil {
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		errs = append(errs, s.lock.Close())
		s.lock = nil
	}
	return errors.Join(errs...)
}

func prepareDir(dir string) error {
	info, err := os.Lstat(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return fmt.Errorf("%w: create state directory: %v", ErrProtected, err)
		}
		info, err = os.Lstat(dir)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrProtected, err)
		}
	case err != nil:
		return fmt.Errorf("%w: %v", ErrProtected, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: state directory is a symlink", ErrProtected)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: state directory is not a directory", ErrProtected)
	}
	if info.Mode().Perm() != dirPerm {
		if err := os.Chmod(dir, dirPerm); err != nil {
			return fmt.Errorf("%w: cannot protect state directory: %v", ErrProtected, err)
		}
	}
	return nil
}

func acquireLock(path string) (*os.File, error) {
	if err := rejectSymlink(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, filePerm)
	if err != nil {
		return nil, fmt.Errorf("%w: open lock: %v", ErrProtected, err)
	}
	if err := f.Chmod(filePerm); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: protect lock: %v", ErrProtected, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("%w: lock: %v", ErrProtected, err)
	}
	return f, nil
}

func rejectSymlink(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrProtected, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s is a symlink", ErrProtected, filepath.Base(path))
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %s is not a regular file", ErrProtected, filepath.Base(path))
	}
	return nil
}

func protectDBFiles(path string) error {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := rejectSymlink(p); err != nil {
			return err
		}
		if err := os.Chmod(p, filePerm); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: protect %s: %v", ErrProtected, filepath.Base(p), err)
		}
	}
	return nil
}

func openPool(path string) (*sql.DB, error) {
	uri := url.URL{Scheme: "file", Path: path, OmitHost: true}
	q := uri.Query()
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	uri.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, fmt.Errorf("%w: open database: %v", ErrStorage, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

// migrateHost applies the versioned host schema transactionally and fails
// closed on unknown or newer versions without touching data.
func migrateHost(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	var version int
	err := db.QueryRowContext(ctx, `SELECT version FROM schema_version WHERE id = 1`).Scan(&version)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		version = 0
	case err != nil:
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	if version > hostSchemaVer {
		return fmt.Errorf("%w: host database version %d is newer than supported %d", ErrSchema, version, hostSchemaVer)
	}
	if version == hostSchemaVer {
		return nil
	}
	if version != 0 {
		return fmt.Errorf("%w: host database version %d has no migration path", ErrSchema, version)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range schemaV1 {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%w: apply schema: %v", ErrStorage, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_version (id, version) VALUES (1, ?)`, hostSchemaVer); err != nil {
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	return nil
}

var schemaV1 = []string{
	`CREATE TABLE conversations (
		route_key TEXT PRIMARY KEY,
		generation INTEGER NOT NULL,
		runtime_session_id TEXT NOT NULL UNIQUE,
		platform TEXT NOT NULL,
		installation TEXT NOT NULL,
		channel TEXT NOT NULL,
		thread_root TEXT NOT NULL,
		dm_actor TEXT NOT NULL,
		creator_actor TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		next_seq INTEGER NOT NULL DEFAULT 1
	)`,
	`CREATE TABLE thread_bindings (
		platform TEXT NOT NULL,
		installation TEXT NOT NULL,
		source_message_id TEXT NOT NULL,
		thread_id TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		PRIMARY KEY (platform, installation, source_message_id)
	)`,
	`CREATE TABLE inbox (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		dedup_key TEXT NOT NULL UNIQUE,
		platform TEXT NOT NULL,
		installation TEXT NOT NULL,
		message_id TEXT NOT NULL,
		route_key TEXT NOT NULL,
		generation INTEGER NOT NULL,
		seq INTEGER NOT NULL,
		kind TEXT NOT NULL,
		actor TEXT NOT NULL,
		actor_label TEXT NOT NULL,
		content TEXT,
		content_hash TEXT NOT NULL,
		files_notice INTEGER NOT NULL DEFAULT 0,
		admission_key TEXT NOT NULL,
		state TEXT NOT NULL,
		run_id TEXT NOT NULL DEFAULT '',
		receipt_user_msg TEXT NOT NULL DEFAULT '',
		receipt_assistant_msg TEXT NOT NULL DEFAULT '',
		run_status TEXT NOT NULL DEFAULT '',
		result_code TEXT NOT NULL DEFAULT '',
		stop_target_run_id TEXT NOT NULL DEFAULT '',
		stop_target_inbox_id INTEGER NOT NULL DEFAULT 0,
		stop_cutoff_seq INTEGER NOT NULL DEFAULT 0,
		attempts INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL
	)`,
	`CREATE INDEX inbox_route_state ON inbox (route_key, state, seq)`,
	`CREATE INDEX inbox_state ON inbox (state)`,
	`CREATE TABLE deliveries (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		route_key TEXT NOT NULL,
		generation INTEGER NOT NULL,
		inbox_id INTEGER NOT NULL,
		run_id TEXT NOT NULL,
		delivery_seq INTEGER NOT NULL,
		chunk_index INTEGER NOT NULL,
		platform TEXT NOT NULL,
		installation TEXT NOT NULL,
		channel TEXT NOT NULL,
		thread_root TEXT NOT NULL,
		remote_id TEXT NOT NULL DEFAULT '',
		nonce TEXT NOT NULL DEFAULT '',
		desired_revision INTEGER NOT NULL DEFAULT 0,
		acked_revision INTEGER NOT NULL DEFAULT -1,
		desired_text TEXT,
		content_hash TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL,
		op_state TEXT NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		first_attempt_at INTEGER NOT NULL DEFAULT 0,
		retry_at INTEGER NOT NULL DEFAULT 0,
		audit TEXT NOT NULL DEFAULT '',
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		UNIQUE (run_id, chunk_index)
	)`,
	`CREATE INDEX deliveries_route ON deliveries (route_key, delivery_seq, chunk_index)`,
	`CREATE INDEX deliveries_status ON deliveries (status)`,
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.host.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	return nil
}

func storageErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrStorage, err)
}
