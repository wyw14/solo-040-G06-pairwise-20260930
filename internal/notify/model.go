package notify

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Severity ranks how urgently a duty receiver must react.
type Severity string

const (
	SeverityNotice   Severity = "notice"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

func (s Severity) Valid() bool {
	switch s {
	case SeverityNotice, SeverityWarning, SeverityCritical:
		return true
	default:
		return false
	}
}

// Rank orders severities so receivers can filter everything at or above a level.
func (s Severity) Rank() int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityNotice:
		return 1
	default:
		return 0
	}
}

// NotificationStatus tracks the delivery lifecycle independently of safety actions.
type NotificationStatus string

const (
	StatusPending   NotificationStatus = "pending"
	StatusDelivered NotificationStatus = "delivered"
	StatusFailed    NotificationStatus = "failed"
)

// ReceiverKind selects the delivery transport for a duty receiver.
type ReceiverKind string

const (
	ReceiverLog     ReceiverKind = "log"
	ReceiverWebhook ReceiverKind = "webhook"
)

// Receiver is a configured duty destination addressed by chamber and minimum severity.
type Receiver struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Kind        ReceiverKind `json:"kind"`
	Target      string       `json:"target,omitempty"`
	Chambers    []string     `json:"chambers,omitempty"`
	MinSeverity Severity     `json:"min_severity"`
	Enabled     bool         `json:"enabled"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

// Matches reports whether a chamber/severity pair must be routed to this receiver.
func (r Receiver) Matches(chamberID string, severity Severity) bool {
	if !r.Enabled {
		return false
	}
	if severity.Rank() < r.MinSeverity.Rank() {
		return false
	}
	if len(r.Chambers) > 0 {
		found := false
		for _, candidate := range r.Chambers {
			if candidate == chamberID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (r Receiver) validate() error {
	if !receiverIDPattern.MatchString(r.ID) {
		return errors.New("receiver id must be 1-64 letters, digits, '_' or '-'")
	}
	if r.Name == "" {
		return errors.New("receiver name is required")
	}
	switch r.Kind {
	case ReceiverLog:
	case ReceiverWebhook:
		if !webhookTargetPattern.MatchString(r.Target) {
			return errors.New("webhook receiver requires an http(s) target URL")
		}
	default:
		return errors.New("receiver kind must be log or webhook")
	}
	if !r.MinSeverity.Valid() {
		return errors.New("receiver min_severity must be notice, warning or critical")
	}
	return nil
}

// Notification is one pending-or-delivered delivery of one event to one receiver.
type Notification struct {
	ID            uuid.UUID          `json:"id"`
	SourceEventID uuid.UUID          `json:"source_event_id"`
	ChamberID     string             `json:"chamber_id"`
	EventType     model.EventType    `json:"event_type"`
	EventSequence uint64             `json:"event_sequence"`
	Severity      Severity           `json:"severity"`
	Title         string             `json:"title"`
	Summary       string             `json:"summary"`
	ReceiverID    string             `json:"receiver_id"`
	Status        NotificationStatus `json:"status"`
	Attempts      int                `json:"attempts"`
	MaxAttempts   int                `json:"max_attempts"`
	NextAttemptAt time.Time          `json:"next_attempt_at"`
	CreatedAt     time.Time          `json:"created_at"`
	OccurredAt    time.Time          `json:"occurred_at"`
	DeliveredAt   *time.Time         `json:"delivered_at,omitempty"`
	LastError     string             `json:"last_error,omitempty"`
	SourcePayload json.RawMessage    `json:"source_payload,omitempty"`
}

// DeliveryAttempt is an append-only record of a single send try.
type DeliveryAttempt struct {
	NotificationID uuid.UUID `json:"notification_id"`
	Attempt        int       `json:"attempt"`
	StartedAt      time.Time `json:"started_at"`
	DurationMS     int64     `json:"duration_ms"`
	Status         string    `json:"status"`
	Detail         string    `json:"detail,omitempty"`
}

// DeliveryRecord joins an attempt with the notification context for the record view.
type DeliveryRecord struct {
	DeliveryAttempt
	SourceEventID uuid.UUID       `json:"source_event_id"`
	ChamberID     string          `json:"chamber_id"`
	Severity      Severity        `json:"severity"`
	Title         string          `json:"title"`
	ReceiverID    string          `json:"receiver_id"`
	ReceiverName  string          `json:"receiver_name"`
	EventType     model.EventType `json:"event_type"`
}
