package journal

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// EventObserver receives every durably appended event. Observers are downstream
// fans-out (for example the notification outbox): their failures are logged
// but never propagated, because a notification problem must not roll back or
// fail the safety action that produced the event.
type EventObserver interface {
	ObserveEvent(ctx context.Context, event model.Event) error
}

// Recorder turns domain transitions into consistently stamped events.
type Recorder struct {
	store     Store
	now       func() time.Time
	mu        sync.RWMutex
	observers []EventObserver
}

func NewRecorder(store Store, now func() time.Time) *Recorder {
	if now == nil {
		now = time.Now
	}
	return &Recorder{store: store, now: now}
}

// Subscribe attaches a downstream observer. Subscriptions made before the
// runtime replays its journal also receive replayed/catch-up events.
func (r *Recorder) Subscribe(observer EventObserver) {
	if observer == nil {
		return
	}
	r.mu.Lock()
	r.observers = append(r.observers, observer)
	r.mu.Unlock()
}

func (r *Recorder) Record(ctx context.Context, chamberID string, kind model.EventType, cycleID uuid.UUID, generation uint64, payload any) (model.Event, error) {
	event, err := model.NewEvent(chamberID, kind, cycleID, generation, payload, r.now())
	if err != nil {
		return model.Event{}, err
	}
	recorded, err := r.store.Append(ctx, event)
	if err != nil {
		return model.Event{}, err
	}
	r.dispatch(ctx, recorded)
	return recorded, nil
}

// dispatch fans the committed event out without ever returning an observer
// error to the caller. A detached context is used so cancellation of the
// triggering HTTP request does not drop the downstream notification intake.
func (r *Recorder) dispatch(ctx context.Context, event model.Event) {
	r.mu.RLock()
	observers := append([]EventObserver(nil), r.observers...)
	r.mu.RUnlock()
	if len(observers) == 0 {
		return
	}
	detached := context.WithoutCancel(ctx)
	for _, observer := range observers {
		// Observers receive events sequentially; each one is expected to
		// enqueue quickly (the outbox only does an idempotent local write).
		if err := observer.ObserveEvent(detached, event); err != nil {
			slog.ErrorContext(detached, "event observer failed; safety action retained",
				"event_id", event.ID, "event_type", event.Type, "error", err)
		}
	}
}

func (r *Recorder) Save(ctx context.Context, snapshot model.ChamberSnapshot) error {
	snapshot.SavedAt = r.now().UTC()
	return r.store.SaveSnapshot(ctx, snapshot)
}

func NewRuntimeStore(dataDirectory string) (Store, error) {
	if dataDirectory == "" {
		return NewMemoryStore(), nil
	}
	return NewFileStore(dataDirectory)
}
