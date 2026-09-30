package notify

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"sync"
	"time"
)

var (
	receiverIDPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	webhookTargetPattern = regexp.MustCompile(`^https?://[^\s]+$`)
)

// ConfigStore persists duty receiver configuration.
type ConfigStore interface {
	LoadReceivers(ctx context.Context) ([]Receiver, error)
	SaveReceivers(ctx context.Context, receivers []Receiver) error
}

// Roster keeps the duty receivers used for routing. It is safe for concurrent use
// and reads never block a safety action: routing takes a point-in-time snapshot.
type Roster struct {
	mu        sync.RWMutex
	store     ConfigStore
	receivers map[string]Receiver
	version   uint64
	now       func() time.Time
}

func NewRoster(store ConfigStore, now func() time.Time) *Roster {
	if now == nil {
		now = time.Now
	}
	return &Roster{store: store, receivers: make(map[string]Receiver), now: now}
}

// Version increments whenever receiver configuration changes, signalling the
// outbox to rescan history so new receivers catch pre-existing safety events.
func (r *Roster) Version() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
}

// Load hydrates the roster from its store.
func (r *Roster) Load(ctx context.Context) error {
	receivers, err := r.store.LoadReceivers(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.receivers = make(map[string]Receiver, len(receivers))
	for _, receiver := range receivers {
		r.receivers[receiver.ID] = receiver
	}
	r.mu.Unlock()
	return nil
}

// Route returns the enabled receivers matching a chamber/severity pair.
func (r *Roster) Route(chamberID string, severity Severity) []Receiver {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var matched []Receiver
	for _, receiver := range r.receivers {
		if receiver.Matches(chamberID, severity) {
			matched = append(matched, receiver)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].ID < matched[j].ID })
	return matched
}

func (r *Roster) List() []Receiver {
	r.mu.RLock()
	result := make([]Receiver, 0, len(r.receivers))
	for _, receiver := range r.receivers {
		result = append(result, receiver)
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (r *Roster) Get(id string) (Receiver, bool) {
	r.mu.RLock()
	receiver, ok := r.receivers[id]
	r.mu.RUnlock()
	return receiver, ok
}

// Upsert validates and stores a receiver, then persists the whole roster.
func (r *Roster) Upsert(ctx context.Context, receiver Receiver) (Receiver, error) {
	if receiver.MinSeverity == "" {
		receiver.MinSeverity = SeverityNotice
	}
	if err := receiver.validate(); err != nil {
		return Receiver{}, err
	}
	stamp := r.now().UTC()
	r.mu.Lock()
	if _, existed := r.receivers[receiver.ID]; !existed {
		r.version++
		receiver.CreatedAt = stamp
	} else {
		receiver.CreatedAt = r.receivers[receiver.ID].CreatedAt
	}
	receiver.UpdatedAt = stamp
	r.receivers[receiver.ID] = receiver
	snapshot := r.snapshotLocked()
	r.mu.Unlock()
	if err := r.store.SaveReceivers(ctx, snapshot); err != nil {
		return Receiver{}, err
	}
	return receiver, nil
}

// SetEnabled turns a receiver on or off without changing the rest of its config.
func (r *Roster) SetEnabled(ctx context.Context, id string, enabled bool) (Receiver, error) {
	r.mu.Lock()
	receiver, ok := r.receivers[id]
	if !ok {
		r.mu.Unlock()
		return Receiver{}, errors.New("receiver not found")
	}
	receiver.Enabled = enabled
	receiver.UpdatedAt = r.now().UTC()
	if r.receivers[id].Enabled != enabled {
		r.version++
	}
	r.receivers[id] = receiver
	snapshot := r.snapshotLocked()
	r.mu.Unlock()
	if err := r.store.SaveReceivers(ctx, snapshot); err != nil {
		return Receiver{}, err
	}
	return receiver, nil
}

func (r *Roster) snapshotLocked() []Receiver {
	result := make([]Receiver, 0, len(r.receivers))
	for _, receiver := range r.receivers {
		result = append(result, receiver)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
