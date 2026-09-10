package discord_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
)

type recordedReq struct {
	Method string
	Path   string
	Body   map[string]any
}

type fakeAPI struct {
	mu    sync.Mutex
	botID string

	channels    map[string]map[string]any
	getMsg      map[string][]map[string]any
	getMsgCalls map[string]int
	lists       map[string][]map[string]any
	nextMsgID   int64

	threadIDs map[string]string
	threadSeq int64

	queues   map[string][]string
	requests []recordedReq

	hangFor time.Duration
}

func (f *fakeAPI) addChannel(id string, typ discordgo.ChannelType, guild, parent string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := map[string]any{"id": id, "type": int(typ)}
	if guild != "" {
		ch["guild_id"] = guild
	}
	if parent != "" {
		ch["parent_id"] = parent
	}
	f.channels[id] = ch
}

func (f *fakeAPI) popStep(method, path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := method + " " + path
	q := f.queues[k]
	if len(q) == 0 {
		return ""
	}
	step := q[0]
	f.queues[k] = q[1:]
	return step
}

func (f *fakeAPI) Requests() []recordedReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedReq, len(f.requests))
	copy(out, f.requests)
	return out
}

func (f *fakeAPI) countRequests(method, path string) int {
	n := 0
	for _, r := range f.Requests() {
		if r.Method == method && r.Path == path {
			n++
		}
	}
	return n
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeAPI) writeStep(w http.ResponseWriter, kind string) bool {
	switch kind {
	case "429":
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"message": "rate limited SENTINEL_BODY", "retry_after": 1.5, "global": false,
		})
		return true
	case "403":
		writeJSON(w, http.StatusForbidden, map[string]any{"message": "Missing Access SENTINEL_BODY", "code": 50001})
		return true
	case "502":
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("bad gateway SENTINEL_BODY"))
		return true
	case "500":
		writeJSON(w, http.StatusInternalServerError, map[string]any{"message": "server error SENTINEL_BODY", "code": 0})
		return true
	case "archived":
		writeJSON(w, http.StatusBadRequest, map[string]any{"message": "Thread is archived SENTINEL_BODY", "code": 50083})
		return true
	case "hang":
		time.Sleep(f.hangFor)
		writeJSON(w, http.StatusOK, map[string]any{})
		return true
	}
	return false
}

func (f *fakeAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if len(raw) != 0 {
			_ = json.Unmarshal(raw, &body)
		}
		f.mu.Lock()
		f.requests = append(f.requests, recordedReq{Method: r.Method, Path: r.URL.Path, Body: body})
		f.mu.Unlock()

		if step := f.popStep(r.Method, r.URL.Path); step != "" {
			if f.writeStep(w, step) {
				return
			}
		}

		rest := strings.TrimPrefix(r.URL.Path, "/api/v9/channels/")
		parts := strings.Split(rest, "/")
		switch {
		case len(parts) == 1 && parts[0] != "":
			id := parts[0]
			switch r.Method {
			case http.MethodGet:
				f.getChannel(w, id)
			case http.MethodPatch:
				f.patchChannel(w, id, body)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		case len(parts) == 2 && parts[1] == "messages":
			cid := parts[0]
			switch r.Method {
			case http.MethodPost:
				f.createMessage(w, cid, body)
			case http.MethodGet:
				f.listMessages(w, cid)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		case len(parts) == 3 && parts[1] == "messages":
			cid, mid := parts[0], parts[2]
			switch r.Method {
			case http.MethodGet:
				f.getMessage(w, cid, mid)
			case http.MethodPatch:
				f.editMessage(w, cid, mid, body)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		case len(parts) == 4 && parts[1] == "messages" && parts[3] == "threads":
			cid, mid := parts[0], parts[2]
			if r.Method == http.MethodPost {
				f.createThread(w, cid, mid)
			} else {
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func (f *fakeAPI) getChannel(w http.ResponseWriter, id string) {
	f.mu.Lock()
	ch, ok := f.channels[id]
	f.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "Unknown Channel SENTINEL_BODY", "code": 10003})
		return
	}
	writeJSON(w, http.StatusOK, ch)
}

func (f *fakeAPI) patchChannel(w http.ResponseWriter, id string, body map[string]any) {
	f.mu.Lock()
	ch, ok := f.channels[id]
	if !ok {
		ch = map[string]any{"id": id, "type": int(discordgo.ChannelTypeGuildPublicThread)}
	}
	for k, v := range body {
		ch[k] = v
	}
	f.channels[id] = ch
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, ch)
}

func (f *fakeAPI) getMessage(w http.ResponseWriter, cid, mid string) {
	key := cid + "/" + mid
	f.mu.Lock()
	versions := f.getMsg[key]
	n := f.getMsgCalls[key]
	f.getMsgCalls[key] = n + 1
	f.mu.Unlock()
	if len(versions) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{"message": "Unknown Message SENTINEL_BODY", "code": 10008})
		return
	}
	idx := n
	if idx >= len(versions) {
		idx = len(versions) - 1
	}
	writeJSON(w, http.StatusOK, versions[idx])
}

func (f *fakeAPI) createThread(w http.ResponseWriter, cid, mid string) {
	key := cid + "/" + mid
	f.mu.Lock()
	tid, ok := f.threadIDs[key]
	if !ok {
		f.threadSeq++
		tid = fmt.Sprintf("%d", 9007199254740993+f.threadSeq)
		f.threadIDs[key] = tid
		guild, _ := f.channels[cid]["guild_id"].(string)
		f.channels[tid] = map[string]any{"id": tid, "type": int(discordgo.ChannelTypeGuildPublicThread), "guild_id": guild, "parent_id": cid}
	}
	ch := f.channels[tid]
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, ch)
}

func (f *fakeAPI) createMessage(w http.ResponseWriter, cid string, body map[string]any) {
	f.mu.Lock()
	id := fmt.Sprintf("%d", f.nextMsgID)
	f.nextMsgID++
	entry := map[string]any{"id": id, "channel_id": cid, "author": map[string]any{"id": f.botID}}
	if body != nil {
		if n, ok := body["nonce"]; ok {
			entry["nonce"] = n
		}
		if c, ok := body["content"]; ok {
			entry["content"] = c
		}
	}
	f.lists[cid] = append(f.lists[cid], entry)
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "channel_id": cid})
}

func (f *fakeAPI) editMessage(w http.ResponseWriter, cid, mid string, body map[string]any) {
	f.mu.Lock()
	for i, m := range f.lists[cid] {
		if fmt.Sprint(m["id"]) == mid {
			if c, ok := body["content"]; ok {
				f.lists[cid][i]["content"] = c
			}
		}
	}
	f.mu.Unlock()
	resp := map[string]any{"id": mid, "channel_id": cid}
	if body != nil {
		if c, ok := body["content"]; ok {
			resp["content"] = c
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (f *fakeAPI) listMessages(w http.ResponseWriter, cid string) {
	f.mu.Lock()
	items := append([]map[string]any(nil), f.lists[cid]...)
	f.mu.Unlock()
	if items == nil {
		items = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, items)
}

// --- test harness ---
