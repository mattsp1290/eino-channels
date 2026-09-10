//go:build race

package agentbridge_test

// raceEnabled skips the 100-turn tests under the race detector; they run in
// the non-race test pass.
const raceEnabled = true
