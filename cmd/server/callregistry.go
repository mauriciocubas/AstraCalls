package main

import (
	"sync"
	"time"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/media"
)

type activeCall struct {
	cm          *call.CallManager
	bridge      *Bridge
	wsBridge    *wsBridge // ponte WebSocket (alternativa ao pion WebRTC para proxies HTTP)
	browserOpus media.Codec
	recorder    *callRecorder // nil quando a gravação está desligada na sessão
	startedAt   time.Time
}

type callRegistry struct {
	mu    sync.Mutex
	calls map[string]*activeCall
}

func newCallRegistry() *callRegistry {
	return &callRegistry{calls: map[string]*activeCall{}}
}

func (r *callRegistry) add(callID string, ac *activeCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ac.startedAt.IsZero() {
		ac.startedAt = time.Now()
	}
	r.calls[callID] = ac
}

// staleRingingIDs: chamadas sem ponte de áudio mais velhas que maxAge (órfãs
// típicas quando o hangup SIP não limpou o registry).
func (r *callRegistry) staleRingingIDs(maxAge time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	var out []string
	for id, ac := range r.calls {
		if ac == nil {
			continue
		}
		if ac.bridge != nil || ac.wsBridge != nil {
			continue
		}
		if now.Sub(ac.startedAt) >= maxAge {
			out = append(out, id)
		}
	}
	return out
}

func (r *callRegistry) get(callID string) (*activeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	return ac, ok
}

func (r *callRegistry) remove(callID string) (*activeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, false
	}
	delete(r.calls, callID)
	return ac, true
}

func (r *callRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *callRegistry) ids() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.calls))
	for id := range r.calls {
		out = append(out, id)
	}
	return out
}

func (r *callRegistry) setBridge(callID string, b *Bridge, oc media.Codec) (*Bridge, media.Codec, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, nil, false
	}
	oldB, oldOC := ac.bridge, ac.browserOpus
	ac.bridge, ac.browserOpus = b, oc
	return oldB, oldOC, true
}

func (r *callRegistry) setWSBridge(callID string, b *wsBridge, oc media.Codec) (*wsBridge, *Bridge, media.Codec, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, nil, nil, false
	}
	oldWSB, oldB, oldOC := ac.wsBridge, ac.bridge, ac.browserOpus
	ac.wsBridge, ac.bridge, ac.browserOpus = b, nil, oc
	return oldWSB, oldB, oldOC, true
}

func (r *callRegistry) drain() []*activeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*activeCall, 0, len(r.calls))
	for _, ac := range r.calls {
		out = append(out, ac)
	}
	r.calls = map[string]*activeCall{}
	return out
}
