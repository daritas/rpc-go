package rpc

import (
	"context"
	"sync"
)

const defaultRememberedOutcomes = 1000

type Invocation struct {
	IdempotencyKey string
	RequestID      string
	Scope          string
	Source         string
	TTL            int64
}

type StoredOutcome struct {
	Result any
	Code   ErrorCode
	Error  *RemoteError
}

func (o StoredOutcome) Failed() bool {
	return o.Code != ""
}

func (o StoredOutcome) Reply(id string) any {
	if o.Failed() {
		return ErrorPayload{ID: id, Type: RPCMessageError, Code: o.Code, Error: o.Error}
	}
	return NewSuccessPayload(id, o.Result)
}

type IdempotencyDecision int

const (
	IdempotencyAcquired IdempotencyDecision = iota
	IdempotencyInProgress
	IdempotencyCompleted
)

type IdempotencyStore interface {
	Begin(context.Context, Invocation) (StoredOutcome, IdempotencyDecision, error)
	Complete(context.Context, Invocation, StoredOutcome) error
}

type IdempotencyAbandoner interface {
	Abandon(context.Context, Invocation) error
}

type MemoryIdempotencyStore struct {
	mu            sync.Mutex
	maxRemembered int
	outcomes      map[string]StoredOutcome
	order         []string
	running       map[string]struct{}
}

func NewMemoryIdempotencyStore(maxRemembered int) *MemoryIdempotencyStore {
	if maxRemembered <= 0 {
		maxRemembered = defaultRememberedOutcomes
	}
	return &MemoryIdempotencyStore{
		maxRemembered: maxRemembered,
		outcomes:      map[string]StoredOutcome{},
		running:       map[string]struct{}{},
	}
}

func (s *MemoryIdempotencyStore) Begin(_ context.Context, invocation Invocation) (StoredOutcome, IdempotencyDecision, error) {
	s.init()
	id := s.id(invocation)

	s.mu.Lock()
	defer s.mu.Unlock()
	if outcome, ok := s.outcomes[id]; ok {
		return outcome, IdempotencyCompleted, nil
	}
	if _, ok := s.running[id]; ok {
		return StoredOutcome{}, IdempotencyInProgress, nil
	}
	s.running[id] = struct{}{}
	return StoredOutcome{}, IdempotencyAcquired, nil
}

func (s *MemoryIdempotencyStore) Complete(_ context.Context, invocation Invocation, outcome StoredOutcome) error {
	s.init()
	id := s.id(invocation)

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.running, id)
	if _, exists := s.outcomes[id]; !exists {
		s.order = append(s.order, id)
	}
	s.outcomes[id] = outcome
	for len(s.order) > s.maxRemembered {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.outcomes, oldest)
	}
	return nil
}

func (s *MemoryIdempotencyStore) Abandon(_ context.Context, invocation Invocation) error {
	s.init()
	s.mu.Lock()
	delete(s.running, s.id(invocation))
	s.mu.Unlock()
	return nil
}

func (s *MemoryIdempotencyStore) init() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxRemembered <= 0 {
		s.maxRemembered = defaultRememberedOutcomes
	}
	if s.outcomes == nil {
		s.outcomes = map[string]StoredOutcome{}
	}
	if s.running == nil {
		s.running = map[string]struct{}{}
	}
}

func (s *MemoryIdempotencyStore) id(invocation Invocation) string {
	return invocation.Scope + "\x00" + invocation.IdempotencyKey
}
