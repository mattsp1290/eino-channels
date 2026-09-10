package slack_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// --- fake Slack Web API ---------------------------------------------------

type fakeCall struct {
	Path string
	Form url.Values
}

type fakeResp struct {
	status     int
	ok         bool
	errCode    string
	retryAfter string
	hang       bool
	ts         string
}

type fakeSlack struct {
	mu sync.Mutex

	authTeamID string
	authUserID string
	authBotID  string
	authOK     bool

	calls       []fakeCall
	postQueue   []fakeResp
	updateQueue []fakeResp
	tsCounter   int
}

func (f *fakeSlack) queuePost(r fakeResp) {
	f.mu.Lock()
	f.postQueue = append(f.postQueue, r)
	f.mu.Unlock()
}

func (f *fakeSlack) queueUpdate(r fakeResp) {
	f.mu.Lock()
	f.updateQueue = append(f.updateQueue, r)
	f.mu.Unlock()
}

func (f *fakeSlack) Calls() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCall(nil), f.calls...)
}

func (f *fakeSlack) lastCall(path string) (fakeCall, bool) {
	calls := f.Calls()
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].Path == path {
			return calls[i], true
		}
	}
	return fakeCall{}, false
}

func (f *fakeSlack) nextTS() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tsCounter++
	return fmt.Sprintf("1700000%03d.000100", f.tsCounter)
}

func (f *fakeSlack) nextResp(isCreate bool) (fakeResp, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := &f.postQueue
	if !isCreate {
		q = &f.updateQueue
	}
	if len(*q) == 0 {
		return fakeResp{}, false
	}
	resp := (*q)[0]
	*q = (*q)[1:]
	return resp, true
}

func (f *fakeSlack) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/")
		f.mu.Lock()
		f.calls = append(f.calls, fakeCall{Path: path, Form: r.Form})
		f.mu.Unlock()

		switch path {
		case "auth.test":
			f.mu.Lock()
			ok, team, user, bot := f.authOK, f.authTeamID, f.authUserID, f.authBotID
			f.mu.Unlock()
			body := map[string]any{"ok": ok, "team_id": team, "user_id": user, "bot_id": bot}
			if !ok {
				body["error"] = "invalid_auth"
			}
			writeJSON(w, http.StatusOK, body)
		case "chat.postMessage":
			f.respond(w, r, true)
		case "chat.update":
			f.respond(w, r, false)
		default:
			writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		}
	})
}

func (f *fakeSlack) respond(w http.ResponseWriter, r *http.Request, isCreate bool) {
	resp, has := f.nextResp(isCreate)
	if !has {
		resp = fakeResp{ok: true}
	}
	if resp.hang {
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
		return
	}
	if resp.status == http.StatusTooManyRequests {
		if resp.retryAfter != "" {
			w.Header().Set("Retry-After", resp.retryAfter)
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	status := resp.status
	if status == 0 {
		status = http.StatusOK
	}
	body := map[string]any{"ok": resp.ok}
	if resp.ok {
		ts := resp.ts
		if ts == "" {
			if isCreate {
				ts = f.nextTS()
			} else {
				ts = r.Form.Get("ts")
			}
		}
		body["channel"] = r.Form.Get("channel")
		body["ts"] = ts
	} else {
		body["error"] = resp.errCode
	}
	writeJSON(w, status, body)
}

// --- harness ---------------------------------------------------------------
