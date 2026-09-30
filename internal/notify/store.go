package notify

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// OutboxStore is the durability boundary for pending notifications.
type OutboxStore interface {
	Load(context.Context) ([]Envelope, error)
	Save(context.Context, []Envelope) error
}

// NewRuntimeStore selects the file-backed outbox when a data directory is
// configured, otherwise the in-memory implementation (mirrors journal setup).
func NewRuntimeStore(dataDirectory string) (OutboxStore, error) {
	if dataDirectory == "" {
		return NewMemoryOutbox(), nil
	}
	return NewFileOutbox(filepath.Join(dataDirectory, "notify"))
}

// MemoryOutbox keeps envelopes in process and is used when no data directory
// was configured (deterministic demo operation).
type MemoryOutbox struct {
	mu        sync.RWMutex
	envelopes map[string]Envelope
}

func NewMemoryOutbox() *MemoryOutbox {
	return &MemoryOutbox{envelopes: make(map[string]Envelope)}
}

func (s *MemoryOutbox) Load(_ context.Context) ([]Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return orderedEnvelopes(s.envelopes), nil
}

func (s *MemoryOutbox) Save(_ context.Context, envelopes []Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.envelopes = make(map[string]Envelope, len(envelopes))
	for _, envelope := range envelopes {
		s.envelopes[envelope.ID] = envelope
	}
	return nil
}

// FileOutbox persists the whole outbox as one atomically replaced JSON
// snapshot. The outbox only ever holds short-lived operational state, so a
// full-state rewrite is simple and crash-safe; the authoritative event history
// stays in the journal.
type FileOutbox struct {
	path string
	mu   sync.Mutex
}

func NewFileOutbox(root string) (*FileOutbox, error) {
	if root == "" {
		return nil, errors.New("notify outbox root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, err
	}
	return &FileOutbox{path: filepath.Join(abs, "outbox.json")}, nil
}

type outboxSnapshot struct {
	Version   int        `json:"version"`
	Envelopes []Envelope `json:"envelopes"`
}

func (s *FileOutbox) Load(_ context.Context) ([]Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var snapshot outboxSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}
	return snapshot.Envelopes, nil
}

func (s *FileOutbox) Save(_ context.Context, envelopes []Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := outboxSnapshot{Version: 1, Envelopes: envelopes}
	encoded, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	temporary := s.path + ".new"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err = file.Write(append(encoded, '\n')); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, s.path)
}

func orderedEnvelopes(values map[string]Envelope) []Envelope {
	result := make([]Envelope, 0, len(values))
	for _, envelope := range values {
		result = append(result, envelope)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		return result[i].ID < result[j].ID
	})
	return result
}
