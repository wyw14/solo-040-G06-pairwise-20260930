package notify

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

// FileStore keeps receivers, the notification outbox and append-only delivery
// records under <root>/notify. Notifications are rewritten atomically; attempts
// are appended so a crash never loses the record of a send.
type FileStore struct {
	dir string
	mu  sync.Mutex
}

func NewFileStore(root string) (*FileStore, error) {
	if root == "" {
		return nil, errors.New("notify store root is required")
	}
	dir := filepath.Join(root, "notify")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) receiversPath() string { return filepath.Join(s.dir, "receivers.json") }
func (s *FileStore) outboxPath() string    { return filepath.Join(s.dir, "notifications.json") }
func (s *FileStore) attemptsPath() string  { return filepath.Join(s.dir, "attempts.jsonl") }

func (s *FileStore) writeJSONAtomic(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".new"
	if err := os.WriteFile(temporary, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (s *FileStore) LoadReceivers(ctx context.Context) ([]Receiver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.receiversPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var receivers []Receiver
	if err := json.Unmarshal(data, &receivers); err != nil {
		return nil, err
	}
	return receivers, nil
}

func (s *FileStore) SaveReceivers(ctx context.Context, receivers []Receiver) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeJSONAtomic(s.receiversPath(), receivers)
}

// outboxSnapshot is the on-disk shape of notifications.json.
type outboxSnapshot struct {
	Notifications []Notification `json:"notifications"`
}

func (s *FileStore) LoadState(ctx context.Context) (map[uuid.UUID]Notification, []DeliveryAttempt, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	notifications := make(map[uuid.UUID]Notification)
	data, err := os.ReadFile(s.outboxPath())
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, nil, err
	default:
		var snapshot outboxSnapshot
		if err := json.Unmarshal(data, &snapshot); err != nil {
			return nil, nil, err
		}
		for _, notification := range snapshot.Notifications {
			notifications[notification.ID] = notification
		}
	}
	attempts, err := s.readAttemptsLocked(ctx)
	if err != nil {
		return nil, nil, err
	}
	return notifications, attempts, nil
}

func (s *FileStore) readAttemptsLocked(ctx context.Context) ([]DeliveryAttempt, error) {
	file, err := os.Open(s.attemptsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var result []DeliveryAttempt
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var attempt DeliveryAttempt
		if err := json.Unmarshal(scanner.Bytes(), &attempt); err != nil {
			return nil, err
		}
		result = append(result, attempt)
	}
	return result, scanner.Err()
}

func (s *FileStore) SaveNotifications(ctx context.Context, items []Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeJSONAtomic(s.outboxPath(), outboxSnapshot{Notifications: items})
}

func (s *FileStore) AppendAttempt(ctx context.Context, attempt DeliveryAttempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := json.Marshal(attempt)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.attemptsPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}
