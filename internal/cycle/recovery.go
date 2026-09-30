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
		lastBusinessSeq := uint64(0)
		businessEvents := 0
		lastWarningTo := uint64(0)
		hasWarning := false
		for _, event := range state.Applied {
			if event.ChamberID != chamberID {
				return nil, fmt.Errorf("event partition mismatch chamber=%s event=%s", chamberID, event.ChamberID)
			}
			if event.Type == model.EventReplayWarning {
				hasWarning = true
				var marker struct {
					ToSequence uint64 `json:"to_sequence"`
				}
				if err := event.Decode(&marker); err == nil && marker.ToSequence >= lastWarningTo {
					lastWarningTo = marker.ToSequence
				}
				continue
			}
			businessEvents++
			lastBusinessSeq = event.Sequence
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
		}
		// State was rebuilt from events newer than the last checkpoint, so the
		// recovery itself deserves operator review. A previous warning that
		// already covers the current business-event cursor suppresses a new
		// one, so repeated restarts never accumulate fresh warnings (and the
		// outbox never mints a new notification for the same recovery).
		replayed := businessEvents
		alreadyCovered := hasWarning && lastWarningTo >= lastBusinessSeq
		if replayed > 0 && !alreadyCovered && r.recorder != nil {
			if _, err := r.recorder.Record(ctx, chamberID, model.EventReplayWarning, uuid.Nil, 0, map[string]any{
				"replayed_events": replayed,
				"from_sequence":   state.Snapshot.Sequence,
				"to_sequence":     lastBusinessSeq,
				"restored_cycle":  cycleValue != nil,
			}); err != nil {
				return nil, err
			}
		}
	}
	return restored, nil
}
