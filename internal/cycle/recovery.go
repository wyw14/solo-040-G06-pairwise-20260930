package cycle

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Recovery rebuilds coordinator state from partition-safe replay results.
type Recovery struct {
	replayer    *journal.Replayer
	coordinator *Coordinator
	recorder    *journal.Recorder
}

func NewRecovery(replayer *journal.Replayer, coordinator *Coordinator, recorder *journal.Recorder) *Recovery {
	return &Recovery{replayer: replayer, coordinator: coordinator, recorder: recorder}
}

func (r *Recovery) RestoreAll(ctx context.Context) (map[string]model.TransitCycle, error) {
	states, err := r.replayer.ReplayAll(ctx)
	if err != nil {
		return nil, err
	}
	restored := make(map[string]model.TransitCycle)
	for chamberID, state := range states {
		cycleValue := state.Snapshot.Cycle
		for _, event := range state.Applied {
			if event.ChamberID != chamberID {
				return nil, fmt.Errorf("event partition mismatch chamber=%s event=%s", chamberID, event.ChamberID)
			}
			if event.Type == model.EventCycleCreated || event.Type == model.EventCycleAdvanced {
				var decoded model.TransitCycle
				if err := event.Decode(&decoded); err != nil {
					return nil, err
				}
				if decoded.ChamberID != chamberID {
					return nil, fmt.Errorf("cycle partition mismatch chamber=%s cycle=%s", chamberID, decoded.ChamberID)
				}
				cycleCopy := decoded
				cycleValue = &cycleCopy
			}
		}
		if cycleValue != nil {
			r.coordinator.Restore(*cycleValue)
			restored[chamberID] = cycleValue.Clone()
			// A transit interrupted mid-passage by a restart is not safe to assume
			// about; raise a persistent recovery warning for duty review. The warning
			// is recorded at most once per transit, so repeated restarts never append
			// duplicate events or cause the notify outbox to emit new notifications.
			if !cycleValue.Stage.Terminal() && state.Cursor > 0 && !hasReplayWarning(state.Applied, cycleValue.ID) {
				if _, err := r.recorder.Record(ctx, chamberID, model.EventReplayWarning, cycleValue.ID, cycleValue.Generation, map[string]any{
					"message":     fmt.Sprintf("transit recovered in non-terminal stage %q; operator review required", cycleValue.Stage),
					"stage":       cycleValue.Stage,
					"replayed_to": state.Cursor,
					"generation":  cycleValue.Generation,
				}); err != nil {
					return nil, err
				}
			}
		}
	}
	return restored, nil
}

func hasReplayWarning(events []model.Event, cycleID uuid.UUID) bool {
	for _, event := range events {
		if event.Type == model.EventReplayWarning && event.CycleID == cycleID {
			return true
		}
	}
	return false
}
