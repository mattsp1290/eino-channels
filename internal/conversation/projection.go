package conversation

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mattsp1290/eino-agent/runtime"
	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"

	"github.com/mattsp1290/eino-channels/internal/agentbridge"
	"github.com/mattsp1290/eino-channels/internal/config"
	"github.com/mattsp1290/eino-channels/internal/redact"
	"github.com/mattsp1290/eino-channels/internal/state"
)

// projection consumes watch updates for one run, coalesces previews and
// enforces the output cap. It owns the single Done consumer.
type projection struct {
	s         *Service
	conv      state.Conversation
	item      state.Item
	deliverer Deliverer
	sub       *watch.Subscription
	handle    runtime.Handle

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	live     string
	lastSeen string // last non-empty observed prefix, kept across LiveUnavailable
	dirty    bool
	limit    bool
	resyncs  int
	previewR *state.Delivery
	lastEdit time.Time
}

func newProjection(s *Service, conv state.Conversation, item state.Item, d Deliverer, sub *watch.Subscription, h runtime.Handle) *projection {
	p := &projection{s: s, conv: conv, item: item, deliverer: d, sub: sub, handle: h}
	p.ctx, p.cancel = context.WithCancel(s.ctx)
	if sub != nil {
		p.wg.Add(2)
		go p.consume()
		go p.flushLoop()
	}
	return p
}

func (p *projection) wait() runtime.Result {
	select {
	case r := <-p.handle.Done():
		return r
	case <-p.s.ctx.Done():
		// Shutdown: give the interrupt a bounded chance to settle.
		select {
		case r := <-p.handle.Done():
			return r
		case <-time.After(5 * time.Second):
			return runtime.Result{RunID: p.handle.RunID(), Status: session.RunInterrupted}
		}
	}
}

func (p *projection) stop() {
	p.cancel()
	p.mu.Lock()
	sub := p.sub
	p.mu.Unlock()
	if sub != nil {
		sub.Close()
	}
	p.wg.Wait()
	// A resync may have swapped the subscription after the close above.
	p.mu.Lock()
	if p.sub != nil && p.sub != sub {
		p.sub.Close()
	}
	p.mu.Unlock()
}

func (p *projection) limitHit() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.limit
}

// liveText returns the last observed transient prefix of this run.
func (p *projection) liveText() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastSeen
}

func (p *projection) consume() {
	defer p.wg.Done()
	runID := p.handle.RunID()
	for {
		p.mu.Lock()
		sub := p.sub
		p.mu.Unlock()
		u, err := sub.Next(p.ctx)
		if err != nil {
			if p.ctx.Err() != nil {
				return
			}
			// Any subscription death (resync required, a store read timeout,
			// capacity) costs the live prefix; re-watch a bounded number of
			// times and say so when giving up.
			if p.resyncs < maxWatchResyncs {
				p.resyncs++
				select {
				case <-time.After(time.Duration(p.resyncs) * 200 * time.Millisecond):
				case <-p.ctx.Done():
					return
				}
				fresh, werr := p.s.bridge.Watch(p.ctx, session.ID(p.conv.RuntimeSessionID))
				if werr == nil {
					p.mu.Lock()
					old := p.sub
					p.sub = fresh
					p.mu.Unlock()
					old.Close()
					continue
				}
			}
			p.s.log.Warn("watch subscription ended; previews unavailable for the rest of this turn", "resync", errors.Is(err, watch.ErrResyncRequired), "error", redact.Err(err))
			return
		}
		switch u.Kind {
		case watch.Live:
			if u.Live.Identity.RunID != runID {
				continue
			}
			p.setLive(u.Live.Text)
		case watch.LiveUnavailable:
			p.setLive("")
		case watch.Durable:
			for _, r := range u.Snapshot.Runs {
				if r.ID == runID && r.Terminal() {
					return
				}
			}
			if text, ok := agentbridge.AssistantText(u.Snapshot, runID); ok {
				p.setLive(text)
			}
		}
	}
}

func (p *projection) setLive(text string) {
	p.mu.Lock()
	if len(text) > config.MaxOutputBytes && !p.limit {
		p.limit = true
		p.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = p.handle.Interrupt(ctx, "output limit")
		cancel()
		return
	}
	if text != p.live {
		p.live, p.dirty = text, true
	}
	if text != "" {
		p.lastSeen = text
	}
	p.mu.Unlock()
}

// flushLoop creates the status message once and edits it with coalesced
// previews at most once per coalescing interval.
func (p *projection) flushLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.s.spacing)
	defer ticker.Stop()
	// Create the placeholder only when this run is next in the lane.
	if !p.ensurePreviewRow() {
		return
	}
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
		}
		p.mu.Lock()
		text, dirty := p.live, p.dirty
		p.dirty = false
		row := p.previewR
		p.mu.Unlock()
		if !dirty || row == nil || row.RemoteID == "" {
			continue
		}
		if err := p.s.throttle(p.ctx, row.Destination); err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(p.ctx, p.s.platformTimeout())
		err := p.deliverer.Edit(ctx, row.Destination, row.RemoteID, p.deliverer.Preview(text))
		cancel()
		if err != nil {
			var de *DeliveryError
			if errors.As(err, &de) && de.Kind == KindRateLimited && de.RetryAfter > 0 {
				select {
				case <-time.After(min(de.RetryAfter, 30*time.Second)):
				case <-p.ctx.Done():
					return
				}
			}
		}
	}
}

// ensurePreviewRow plans and creates the chunk-0 status message when the
// delivery lane is clear up to this run. It returns false when previews
// must be skipped for this turn.
func (p *projection) ensurePreviewRow() bool {
	// The recheck is a precondition of every dispatch, previews included.
	actx, acancel := context.WithTimeout(p.ctx, p.s.platformTimeout())
	allowed, aerr := p.deliverer.Allowed(actx, p.conv.Route.Destination(), p.conv.Route.Subject())
	acancel()
	if aerr != nil || !allowed {
		return false
	}
	row, err := p.s.st.PlanPreview(p.ctx, p.item, p.deliverer.Preview(""))
	if err != nil {
		return false
	}
	next, blocked, err := p.s.st.NextDelivery(p.ctx, p.conv.Route.Key())
	if err != nil || blocked || next.ID != row.ID {
		return false
	}
	if row.RemoteID == "" {
		// A create in flight must not be canceled by run completion: that is
		// exactly the ambiguous case. Persistence after a create is never canceled.
		if p.s.createDelivery(context.WithoutCancel(p.ctx), p.deliverer, row) != createSucceeded {
			return false
		}
		created, err := p.s.st.GetDelivery(p.ctx, row.ID)
		if err != nil || created.RemoteID == "" {
			return false
		}
		row = created
	}
	p.mu.Lock()
	p.previewR = &row
	p.mu.Unlock()
	return true
}
