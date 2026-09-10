package conversation

import (
	"time"

	"github.com/mattsp1290/eino-channels/internal/redact"
	"github.com/mattsp1290/eino-channels/internal/state"
)

const (
	scheduleTick     = time.Second
	routeBatch       = 64
	maxIngestEntries = 4096
	parkBlocked      = 10 * time.Minute // operator resolution restarts the service anyway
	parkUnavailable  = 15 * time.Second
)

// scheduler spawns bounded route runners for routes that have work.
func (s *Service) scheduler() {
	defer s.loops.Done()
	ticker := time.NewTicker(scheduleTick)
	defer ticker.Stop()
	cursor := "" // rotation position; owned by this goroutine
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C:
		}
		// An ingest scans from the start so the changed route is not behind
		// the cursor; runner completions keep the rotation.
		s.mu.Lock()
		rescan, closing := s.rescan, s.closing
		s.rescan = false
		s.mu.Unlock()
		if closing {
			return
		}
		if rescan {
			cursor = ""
		}
		keys, err := s.st.RoutesWithWork(s.ctx, cursor, routeBatch)
		if err == nil && len(keys) == 0 && cursor != "" {
			cursor = ""
			keys, err = s.st.RoutesWithWork(s.ctx, "", routeBatch)
		}
		if err != nil {
			s.log.Warn("scheduler scan failed", "error", redact.Err(err))
			continue
		}
		now := time.Now()
		full := false
		for _, key := range keys {
			s.mu.Lock()
			if len(s.active) >= s.limits.MaxRunningConversations {
				s.mu.Unlock()
				full = true
				break // the cursor stays before this key so it is attempted next
			}
			cursor = key
			_, running := s.active[key]
			until, parked := s.parked[key]
			if running || parked && now.Before(until) {
				s.mu.Unlock()
				continue
			}
			s.active[key] = struct{}{}
			delete(s.parked, key)
			s.mu.Unlock()
			s.runners.Add(1)
			go s.runRoute(key)
		}
		if !full && len(keys) < routeBatch {
			cursor = "" // the batch was exhausted; next scan starts over
		}
	}
}

// park delays rescheduling a route unless an ingest arrived for it since
// the runner last observed the route (seen). It reports whether it parked.
func (s *Service) park(key string, d time.Duration, seen uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ingests[key] != seen {
		return false
	}
	s.parked[key] = time.Now().Add(d)
	return true
}

// unpark clears a park and bumps the ingest counter so a concurrent park
// decision made on stale information is refused.
func (s *Service) unpark(key string) {
	s.mu.Lock()
	delete(s.parked, key)
	s.ingests[key]++
	s.rescan = true
	if len(s.ingests) > maxIngestEntries {
		// Drop counters for idle routes; a dropped counter makes a stale
		// park decision refuse to park, which is the safe direction.
		for k := range s.ingests {
			_, active := s.active[k]
			_, parked := s.parked[k]
			if !active && !parked {
				delete(s.ingests, k)
			}
		}
	}
	s.mu.Unlock()
}

func (s *Service) ingestSeq(key string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ingests[key]
}

// runRoute processes one route until it is idle, parked or closing.
func (s *Service) runRoute(key string) {
	defer s.runners.Done()
	defer func() {
		s.mu.Lock()
		delete(s.active, key)
		s.mu.Unlock()
		s.signal()
	}()
	conv, err := s.st.GetConversationByKey(s.ctx, key)
	if err != nil {
		s.log.Warn("route unavailable", "error", redact.Err(err))
		s.park(key, parkUnavailable, s.ingestSeq(key))
		return
	}
	for {
		s.mu.Lock()
		closing := s.closing
		s.mu.Unlock()
		if closing || s.ctx.Err() != nil {
			return
		}
		seen := s.ingestSeq(key)
		if current, err := s.st.GetConversationByKey(s.ctx, key); err == nil {
			conv = current
		}
		item, err := s.st.NextWork(s.ctx, key)
		switch {
		case err == nil:
			var outcome workOutcome
			switch item.Kind {
			case state.KindStop:
				outcome = s.settleStop(conv, item)
			case state.KindPrompt:
				outcome = s.runPrompt(&conv, item)
			default:
				_ = s.st.Transition(s.ctx, item.ID, "", state.StateComplete, "")
				outcome = workDone
			}
			if outcome == workPark {
				if s.park(key, parkUnavailable, seen) {
					return
				}
				continue
			}
			if outcome == workStop {
				return
			}
			continue
		case !isNotFound(err):
			s.log.Warn("next work failed", "error", redact.Err(err))
			if s.park(key, parkUnavailable, seen) {
				return
			}
			continue
		}
		// No inbox work: drain the delivery lane.
		blocked, wait := s.drainDeliveries(&conv)
		switch {
		case blocked:
			if s.park(key, parkBlocked, seen) {
				return
			}
			continue
		case wait > 0:
			if s.park(key, wait, seen) {
				return
			}
			continue
		}
		if s.ingestSeq(key) != seen {
			continue // work arrived while draining
		}
		return
	}
}

type workOutcome int

const (
	workDone workOutcome = iota
	workPark
	workStop
)
