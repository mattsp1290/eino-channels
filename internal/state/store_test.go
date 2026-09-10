package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "state")
}

// openSafely calls Open and recovers any panic so a single confirmed
// production bug cannot crash the whole test binary (a real panic aborts
// the entire `go test` run, not just the current test).
//
// BUG (reported, not fixed): Open's named return value `st` is set to
// `&Store{...}` and a deferred cleanup closure captures it to call
// `st.Close()` on error. Every error path after that point does
// `return nil, err`, which reassigns the named return `st` to nil before
// the defer runs — so `st.Close()` panics on a nil receiver instead of
// returning the documented sentinel error. See store.go around lines
// 74-113 (every `return nil, err` in that range) and the panic inside
// Close() at store.go:127 (`s.agentDB != nil` dereferencing nil s).
func openSafely(t *testing.T, ctx context.Context, dir string, opts Options) (st *Store, err error, panicked any) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			panicked = r
		}
	}()
	st, err = Open(ctx, dir, opts)
	return
}

// 1. Open creates the directory and files with the expected permissions, and
// a second Open in the same process fails with ErrLocked (flock is per open
// file description, so a second OpenFile+flock in-process conflicts with the
// first).
func TestOpenCreatesProtectedFilesAndLocksInProcess(t *testing.T) {
	dir := testDir(t)
	ctx := context.Background()

	st, err := Open(ctx, dir, Options{SkipAgentDB: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != dirPerm {
		t.Errorf("dir perm = %o, want %o", perm, dirPerm)
	}

	lockInfo, err := os.Stat(filepath.Join(dir, LockFile))
	if err != nil {
		t.Fatalf("stat lock: %v", err)
	}
	if perm := lockInfo.Mode().Perm(); perm != filePerm {
		t.Errorf("lock perm = %o, want %o", perm, filePerm)
	}

	dbInfo, err := os.Stat(filepath.Join(dir, HostDBFile))
	if err != nil {
		t.Fatalf("stat db: %v", err)
	}
	if perm := dbInfo.Mode().Perm(); perm != filePerm {
		t.Errorf("db perm = %o, want %o", perm, filePerm)
	}

	_, err, panicked := openSafely(t, ctx, dir, Options{SkipAgentDB: true})
	if panicked != nil {
		t.Fatalf("second in-process Open panicked instead of returning ErrLocked (production bug, see openSafely doc comment): %v", panicked)
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second in-process Open err = %v, want ErrLocked", err)
	}
}

// 2. TestLockHelper is a re-exec helper: when invoked with
// EINO_CHANNELS_LOCK_HELPER=1 it attempts to Open the directory named by
// EINO_CHANNELS_LOCK_DIR and prints LOCKED or OK. With the env var unset it
// returns immediately, so it is a no-op under a normal `go test` run.
func TestLockHelper(t *testing.T) {
	if os.Getenv("EINO_CHANNELS_LOCK_HELPER") != "1" {
		return
	}
	dir := os.Getenv("EINO_CHANNELS_LOCK_DIR")
	ctx := context.Background()
	st, err := Open(ctx, dir, Options{SkipAgentDB: true})
	if err != nil {
		if errors.Is(err, ErrLocked) {
			fmt.Println("LOCKED")
			return
		}
		fmt.Println("ERROR:", err)
		return
	}
	defer st.Close()
	fmt.Println("OK")
}

func runLockHelper(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockHelper$")
	cmd.Env = append(os.Environ(),
		"EINO_CHANNELS_LOCK_HELPER=1",
		"EINO_CHANNELS_LOCK_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("lock helper process exit: %v", err)
	}
	return string(out)
}

func TestLockAcrossProcesses(t *testing.T) {
	dir := testDir(t)
	ctx := context.Background()

	st, err := Open(ctx, dir, Options{SkipAgentDB: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	out := runLockHelper(t, dir)
	if !strings.Contains(out, "LOCKED") {
		t.Fatalf("expected LOCKED in helper output while holding the lock, got: %s", out)
	}

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	out = runLockHelper(t, dir)
	if !strings.Contains(out, "OK") {
		t.Fatalf("expected OK in helper output after releasing the lock, got: %s", out)
	}
}

// 3. Symlink rejection: the state dir itself, a pre-created channels.db, and
// a pre-created lock file must all be rejected with ErrProtected.
func TestOpenRejectsSymlinkStateDir(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatalf("mkdir real: %v", err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, err := Open(context.Background(), link, Options{SkipAgentDB: true})
	if !errors.Is(err, ErrProtected) {
		t.Fatalf("err = %v, want ErrProtected", err)
	}
}

func TestOpenRejectsSymlinkHostDB(t *testing.T) {
	dir := testDir(t)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir dir: %v", err)
	}
	target := filepath.Join(t.TempDir(), "target.db")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, HostDBFile)); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, err, panicked := openSafely(t, context.Background(), dir, Options{SkipAgentDB: true})
	if panicked != nil {
		t.Fatalf("Open panicked instead of returning ErrProtected (production bug, see openSafely doc comment): %v", panicked)
	}
	if !errors.Is(err, ErrProtected) {
		t.Fatalf("err = %v, want ErrProtected", err)
	}
}

func TestOpenRejectsSymlinkLock(t *testing.T) {
	dir := testDir(t)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir dir: %v", err)
	}
	target := filepath.Join(t.TempDir(), "target.lock")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, LockFile)); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, err, panicked := openSafely(t, context.Background(), dir, Options{SkipAgentDB: true})
	if panicked != nil {
		t.Fatalf("Open panicked instead of returning ErrProtected (production bug, see openSafely doc comment): %v", panicked)
	}
	if !errors.Is(err, ErrProtected) {
		t.Fatalf("err = %v, want ErrProtected", err)
	}
}

// 4. Relative or unclean paths are rejected.
func TestOpenRejectsRelativeOrUncleanPath(t *testing.T) {
	ctx := context.Background()

	if _, err := Open(ctx, "relative/state-dir", Options{SkipAgentDB: true}); !errors.Is(err, ErrProtected) {
		t.Fatalf("relative path err = %v, want ErrProtected", err)
	}

	dir := testDir(t)
	unclean := dir + string(filepath.Separator) + "."
	if _, err := Open(ctx, unclean, Options{SkipAgentDB: true}); !errors.Is(err, ErrProtected) {
		t.Fatalf("unclean path err = %v, want ErrProtected", err)
	}
}

// 5. Schema fail-closed: a newer schema_version is rejected without
// touching the existing tables.
func TestOpenFailsClosedOnNewerSchemaVersion(t *testing.T) {
	dir := testDir(t)
	ctx := context.Background()

	st, err := Open(ctx, dir, Options{SkipAgentDB: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dbPath := filepath.Join(dir, HostDBFile)
	raw, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `UPDATE schema_version SET version = 99`); err != nil {
		t.Fatalf("bump schema version: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	_, err, panicked := openSafely(t, ctx, dir, Options{SkipAgentDB: true})
	if panicked != nil {
		t.Fatalf("reopen panicked instead of returning ErrSchema (production bug, see openSafely doc comment): %v", panicked)
	}
	if !errors.Is(err, ErrSchema) {
		t.Fatalf("reopen err = %v, want ErrSchema", err)
	}

	raw2, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("sql.Open verify: %v", err)
	}
	defer raw2.Close()
	var name string
	if err := raw2.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'conversations'`).Scan(&name); err != nil {
		t.Fatalf("conversations table missing after failed reopen: %v", err)
	}
	if name != "conversations" {
		t.Fatalf("unexpected table name %q", name)
	}
}

// 6. Reopen: data survives Close/Open.
func TestReopenPreservesConversationAndInbox(t *testing.T) {
	dir := testDir(t)
	ctx := context.Background()

	st, err := Open(ctx, dir, Options{SkipAgentDB: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	route := Route{Platform: PlatformSlack, Installation: "T1", Channel: "C1", ThreadRoot: "1.1"}
	in := Inbound{Route: route, MessageID: "1.1", Actor: "U1", ActorLabel: "U1", Kind: KindPrompt, Content: "hello", ReceivedAt: time.Now()}
	disp, err := st.Ingest(ctx, in, Capacity{MaxQueuedPerRoute: 8, MaxPendingGlobal: 256})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if disp.Outcome != OutcomeAccepted {
		t.Fatalf("outcome = %v, want accepted", disp.Outcome)
	}
	wantSessionID := disp.Conversation.RuntimeSessionID
	itemID := disp.Item.ID

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st2, err := Open(ctx, dir, Options{SkipAgentDB: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()

	conv, err := st2.GetConversation(ctx, route)
	if err != nil {
		t.Fatalf("GetConversation: %v", err)
	}
	if conv.RuntimeSessionID != wantSessionID {
		t.Fatalf("runtime session id = %q, want %q", conv.RuntimeSessionID, wantSessionID)
	}

	item, err := st2.GetItem(ctx, itemID)
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if item.State != StateQueued {
		t.Fatalf("item state = %v, want queued", item.State)
	}
}

// One test with SkipAgentDB: false to prove Agent() is non-nil and a reopen
// works. This migrates the real eino-agent schema and may take a second.
func TestOpenWithAgentDB(t *testing.T) {
	dir := testDir(t)
	ctx := context.Background()

	st, err := Open(ctx, dir, Options{SkipAgentDB: false})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if st.Agent() == nil {
		t.Fatal("Agent() = nil, want non-nil store")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st2, err := Open(ctx, dir, Options{SkipAgentDB: false})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if st2.Agent() == nil {
		t.Fatal("Agent() = nil after reopen")
	}
}
