package session

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

type Registry struct {
	sessions map[string]*Session
	mu       sync.RWMutex
	sequence atomic.Uint64
}

func (s *Session) accountKey() string {
	id := s.client.IdentityData()
	if id.XUID != "" {
		return id.XUID
	}
	return id.Identity
}

// register commits a completed login. A failed or slower, older login cannot
// retire a newer route. Product admission must run before LoginContext.
func (r *Registry) register(s *Session) error {
	r.mu.Lock()
	if s.ctx.Err() != nil {
		r.mu.Unlock()
		return s.ctx.Err()
	}
	key := s.accountKey()
	previous := r.sessions[key]
	if previous != nil && previous.loginSequence > s.loginSequence {
		r.mu.Unlock()
		return errors.New("login superseded by a newer connection")
	}
	r.sessions[key] = s
	r.mu.Unlock()
	if previous != nil && previous != s {
		previous.Disconnect("Logged in from another connection.")
	}
	return nil
}

func NewRegistry() *Registry {
	return &Registry{
		sessions: make(map[string]*Session),
	}
}

func (r *Registry) AddSession(xuid string, session *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[xuid] = session
}

func (r *Registry) GetSession(xuid string) *Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessions[xuid]
}

func (r *Registry) GetSessionByUsername(username string) *Session {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, session := range r.sessions {
		if strings.EqualFold(session.client.IdentityData().DisplayName, username) {
			return session
		}
	}
	return nil
}

func (r *Registry) RemoveSession(xuid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, xuid)
}

// RemoveSessionIfCurrent retires one connection without removing a newer login
// for the same account. A failed login was never registered and must not remove
// the account's existing session either.
func (r *Registry) RemoveSessionIfCurrent(xuid string, session *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessions[xuid] == session {
		delete(r.sessions, xuid)
	}
}

func (r *Registry) GetSessions() []*Session {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sessions := make([]*Session, 0)
	for _, session := range r.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}
