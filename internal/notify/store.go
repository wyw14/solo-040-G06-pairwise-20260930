package notify

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"
)

// OutboxStore persists pending notifications and append-only delivery records.
type OutboxStore interface {
	LoadState(ctx context.Context) (map[uuid.UUID]Notification, []DeliveryAttempt, error)
	SaveNotifications(ctx context.Context, notifications []Notification) error
	AppendAttempt(ctx context.Context, attempt DeliveryAttempt) error
}

// MemoryOutbox is the non-durable boundary used when no data directory is set.
type MemoryOutbox struct {
	mu            sync.RWMutex
	notifications map[uuid.UUID]Notification
	attempts      []DeliveryAttempt
}

func NewMemoryOutbox() *MemoryOutbox {
	return &MemoryOutbox{notifications: make(map[uuid.UUID]Notification)}
}

func (s *MemoryOutbox) LoadState(ctx context.Context) (map[uuid.UUID]Notification, []DeliveryAttempt, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	notifications := make(map[uuid.UUID]Notification, len(s.notifications))
	for id, notification := range s.notifications {
		notifications[id] = notification
	}
	attempts := append([]DeliveryAttempt(nil), s.attempts...)
	return notifications, attempts, nil
}

func (s *MemoryOutbox) SaveNotifications(ctx context.Context, notifications []Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.notifications = make(map[uuid.UUID]Notification, len(notifications))
	for _, notification := range notifications {
		s.notifications[notification.ID] = notification
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryOutbox) AppendAttempt(ctx context.Context, attempt DeliveryAttempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.attempts = append(s.attempts, attempt)
	s.mu.Unlock()
	return nil
}

// sortedNotifications returns notifications in a stable chronological order for views.
func sortedNotifications(items map[uuid.UUID]Notification) []Notification {
	result := make([]Notification, 0, len(items))
	for _, item := range items {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		return result[i].ID.String() < result[j].ID.String()
	})
	return result
}

// MemoryConfigStore is the non-durable receiver config boundary.
type MemoryConfigStore struct {
	mu        sync.RWMutex
	receivers []Receiver
}

func NewMemoryConfigStore() *MemoryConfigStore { return &MemoryConfigStore{} }

func (s *MemoryConfigStore) LoadReceivers(ctx context.Context) ([]Receiver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	result := append([]Receiver(nil), s.receivers...)
	s.mu.RUnlock()
	return result, nil
}

func (s *MemoryConfigStore) SaveReceivers(ctx context.Context, receivers []Receiver) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.receivers = append([]Receiver(nil), receivers...)
	s.mu.Unlock()
	return nil
}

// NewRuntimeStores returns durable stores rooted under dataDirectory, or memory
// stores when no directory is configured.
func NewRuntimeStores(dataDirectory string) (ConfigStore, OutboxStore, error) {
	if dataDirectory == "" {
		return NewMemoryConfigStore(), NewMemoryOutbox(), nil
	}
	fileStore, err := NewFileStore(dataDirectory)
	if err != nil {
		return nil, nil, err
	}
	return fileStore, fileStore, nil
}
