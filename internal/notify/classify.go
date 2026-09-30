package notify

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// notificationNamespace is fixed so the same (event, receiver) always derives
// the same notification id, across processes and repeated journal scans.
var notificationNamespace = uuid.MustParse("5f0b3a2c-7e91-4d34-9c22-8a6b5e4d1f07")

// DeriveID deterministically maps a source event and receiver to a notification id.
func DeriveID(sourceEventID uuid.UUID, receiverID string) uuid.UUID {
	return uuid.NewSHA1(notificationNamespace, []byte(sourceEventID.String()+"|"+receiverID))
}

type eventMeaning struct {
	severity Severity
	title    string
	summary  string
}

// classifyEvent turns a domain event into notification content.
// Only safety-relevant events are routed; everything else returns false.
func classifyEvent(event model.Event) (eventMeaning, bool) {
	switch event.Type {
	case model.EventEmergencyStarted:
		return eventMeaning{
			severity: SeverityCritical,
			title:    "Emergency stop requested",
			summary:  emergencySummary(event, "cancelling active actuator commands"),
		}, true
	case model.EventEmergencySettled:
		return eventMeaning{
			severity: SeverityWarning,
			title:    "Emergency equipment settled",
			summary:  emergencySummary(event, "equipment reached a stable state"),
		}, true
	case model.EventPlanCancelled:
		return eventMeaning{
			severity: SeverityWarning,
			title:    "Convoy plan cancelled",
			summary:  cancellationSummary(event),
		}, true
	case model.EventReplayWarning:
		return eventMeaning{
			severity: SeverityWarning,
			title:    "Recovery review warning",
			summary:  replaySummary(event),
		}, true
	default:
		return eventMeaning{}, false
	}
}

func emergencySummary(event model.Event, fallback string) string {
	var record model.EmergencyRecord
	if err := json.Unmarshal(event.Payload, &record); err == nil && record.Message != "" {
		fallback = record.Message
	}
	return fmt.Sprintf("[%s] %s", event.ChamberID, fallback)
}

func cancellationSummary(event model.Event) string {
	var payload struct {
		Note               string `json:"note"`
		RestoredQueue      int    `json:"restored_queue"`
		CompensatedSignals int    `json:"compensated_signals"`
	}
	note := "plan cancelled"
	if err := json.Unmarshal(event.Payload, &payload); err == nil {
		if payload.Note != "" {
			note = payload.Note
		}
		return fmt.Sprintf("[%s] %s (queue restored=%d, signals compensated=%d)",
			event.ChamberID, note, payload.RestoredQueue, payload.CompensatedSignals)
	}
	return fmt.Sprintf("[%s] %s", event.ChamberID, note)
}

func replaySummary(event model.Event) string {
	var payload struct {
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	detail := "recovery sequence requires operator review"
	if err := json.Unmarshal(event.Payload, &payload); err == nil {
		switch {
		case payload.Message != "":
			detail = payload.Message
		case payload.Reason != "":
			detail = payload.Reason
		}
	}
	return fmt.Sprintf("[%s] %s", event.ChamberID, detail)
}
