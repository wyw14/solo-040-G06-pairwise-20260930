package cycle

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Recovery must raise one replay.warning when state is rebuilt from
// unsnapshotted events, and must not raise another on a subsequent restart
// that covers the same business-event cursor.
func TestRestoreAllWarnsOnceAcrossRestarts(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	now := func() time.Time { return current }
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, now)

	cycleValue, err := model.NewTransitCycle("chamber-1", uuid.New(), model.DirectionDownstream, 1, current)
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	// Simulate a prior run that persisted a cycle event without snapshotting.
	if _, err := recorder.Record(ctx, "chamber-1", model.EventCycleCreated, cycleValue.ID, 1, cycleValue); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	newRecovery := func() *Recovery {
		gates := gate.NewController(gate.NewState("chamber-1"), gate.NewCommands(), recorder, now)
		ramp := hydraulics.NewRamp(time.Millisecond, now)
		hydro := hydraulics.NewController(gate.NewCommands(), recorder, ramp, now)
		coordinator := NewCoordinator(gates, hydro, recorder, now)
		return NewRecovery(journal.NewReplayer(store), coordinator, recorder)
	}

	if _, err := newRecovery().RestoreAll(ctx); err != nil {
		t.Fatalf("first restore: %v", err)
	}
	events, err := store.Events(ctx, "chamber-1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if warnings := countWarnings(events); warnings != 1 {
		t.Fatalf("expected exactly 1 replay warning after first restore, got %d", warnings)
	}

	// Second restart replays the business event plus the existing warning.
	if _, err := newRecovery().RestoreAll(ctx); err != nil {
		t.Fatalf("second restore: %v", err)
	}
	events, _ = store.Events(ctx, "chamber-1", 0)
	if warnings := countWarnings(events); warnings != 1 {
		t.Fatalf("repeated restart must not mint another warning, got %d", warnings)
	}

	// A new business event past the covered cursor warrants a fresh warning.
	current = current.Add(time.Minute)
	advanced := cycleValue
	if err := advanced.Advance(model.StageAdmitting, "test", current); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if _, err := recorder.Record(ctx, "chamber-1", model.EventCycleAdvanced, advanced.ID, 1, advanced); err != nil {
		t.Fatalf("advance event: %v", err)
	}
	if _, err := newRecovery().RestoreAll(ctx); err != nil {
		t.Fatalf("third restore: %v", err)
	}
	events, _ = store.Events(ctx, "chamber-1", 0)
	if warnings := countWarnings(events); warnings != 2 {
		t.Fatalf("expected a fresh warning for new business events, got %d", warnings)
	}
}

func countWarnings(events []model.Event) int {
	count := 0
	for _, event := range events {
		if event.Type == model.EventReplayWarning {
			count++
		}
	}
	return count
}
