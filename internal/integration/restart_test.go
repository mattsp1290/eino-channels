package integration_test

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/mattsp1290/eino-channels/internal/state"
)

// resetToAdmitted rewinds an inbox row to the admitted state and removes
// its delivery rows, simulating a crash after the agent committed the
// terminal run but before the host recorded it.
func resetToAdmitted(t *testing.T, dir string, id int64) error {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: filepath.Join(dir, state.HostDBFile), OmitHost: true}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(`DELETE FROM deliveries WHERE inbox_id = ?`, id); err != nil {
		return err
	}
	_, err = db.Exec(`UPDATE inbox SET state = ?, run_status = '', result_code = '', content = 'commit' WHERE id = ?`, state.StateAdmitted, id)
	return err
}
