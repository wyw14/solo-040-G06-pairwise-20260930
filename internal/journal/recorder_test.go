package journal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type recordingObserver struct {
	mu     sync.Mutex
	events []model.Event
	fail   bool
}

func (o *recordingObserver) ObserveEvent(_ context.Context, event model.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
	if o.fail {
		return errors.New("downstream unavailable")
	}
	return nil
}

func (o *recordingObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.events)
}

// A failing observer must not roll back the committed event or surface an
// error: notification delivery can never undo a safety action.
func TestRecordSucceedsWhenObserverFails(t *testing.T) {
	store := NewMemoryStore()
	recorder := NewRecorder(store, func() time.Time {
		return time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	})
	observer := &recordingObserver{fail: true}
	recorder.Subscribe(observer)

	event, err := recorder.Record(context.Background(), "chamber-1", model.EventEmergencyStarted, uuid.Nil, 0, map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("Record must succeed despite observer failure, got %v", err)
	}
	persisted, err := store.Events(context.Background(), "chamber-1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(persisted) != 1 || persisted[0].ID != event.ID {
		t.Fatalf("safety event was not retained despite observer failure: %+v", persisted)
	}
	if observer.count() != 1 {
		t.Fatalf("observer should still have been invoked once")
	}
}

// Multiple observers each receive the committed event; one failure does not
// stop the others.
func TestRecordFansOutToAllObservers(t *testing.T) {
	store := NewMemoryStore()
	recorder := NewRecorder(store, nil)
	failing := &recordingObserver{fail: true}
	healthy := &recordingObserver{}
	recorder.Subscribe(failing)
	recorder.Subscribe(healthy)

	if _, err := recorder.Record(context.Background(), "chamber-1", model.EventPlanCancelled, uuid.Nil, 0, nil); err != nil {
		t.Fatalf("record: %v", err)
	}
	if failing.count() != 1 || healthy.count() != 1 {
		t.Fatalf("fan-out incomplete: failing=%d healthy=%d", failing.count(), healthy.count())
	}
	if healthy.events[0].Type != model.EventPlanCancelled {
		t.Fatalf("healthy observer got wrong event: %s", healthy.events[0].Type)
	}
}
