package conversation

import (
	"time"

	"github.com/mattsp1290/eino-channels/internal/state"
)

const (
	scheduleTick   = time.Second
	routeBatch     = 64
	parkBlocked    = 30 * time.Second
	parkUnavailble = 15 * time.Second
)

// scheduler spawns bounded route runners for routes that have work.
func (s *Service) scheduler() {
	defer s.loops.Done()
	ticker := time.NewTicker(scheduleTick)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C:
		}
		s.mu.Lock()
		closing := s.closing
		s.mu.Unlock()
		if closing {
			return
		}
		keys, err := s.st.RoutesWithWork(s.ctx, routeBatch)
		if err != nil {
			s.log.Warn("scheduler scan failed", "error", safeErr(err))
			continue
		}
		now := time.Now()
		for _, key := range keys {
			s.mu.Lock()
			_, running := s.active[key]
			until, parked := s.parked[key]
			if running || parked && now.Before(until) || len(s.active) >= s.limits.MaxRunningConversations {
				s.mu.Unlock()
				continue
			}
			s.active[key] = struct{}{}
			delete(s.parked, key)
			s.mu.Unlock()
			s.runners.Add(1)
			go s.runRoute(key)
		}
	}
}

// park delays rescheduling a route. A new ingest clears the park.
func (s *Service) park(key string, d time.Duration) {
	s.mu.Lock()
	s.parked[key] = time.Now().Add(d)
	s.mu.Unlock()
}

func (s *Service) unpark(key string) {
	s.mu.Lock()
	delete(s.parked, key)
	s.mu.Unlock()
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
		s.log.Warn("route unavailable", "error", safeErr(err))
		s.park(key, parkUnavailble)
		return
	}
	for {
		s.mu.Lock()
		closing := s.closing
		s.mu.Unlock()
		if closing || s.ctx.Err() != nil {
			return
		}
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
				s.park(key, parkUnavailble)
				return
			}
			if outcome == workStop {
				return
			}
			continue
		case !isNotFound(err):
			s.log.Warn("next work failed", "error", safeErr(err))
			s.park(key, parkUnavailble)
			return
		}
		// No inbox work: drain the delivery lane.
		blocked, wait := s.drainDeliveries(&conv)
		switch {
		case blocked:
			s.park(key, parkBlocked)
			return
		case wait > 0:
			s.park(key, wait)
			return
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
