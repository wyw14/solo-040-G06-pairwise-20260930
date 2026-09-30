// Package notify converts persisted domain events (emergency actions, plan
// cancellations, recovery warnings) into duty notifications and delivers them
// to configured on-call sinks through a durable, retryable outbox.
//
// The pipeline is deliberately decoupled from the safety path:
//   - notification identity is derived from the originating event, so the same
//     event produces the same notification after a process restart;
//   - delivery failures never propagate back to the code that performed the
//     safety action — notifications stay pending and are retried;
//   - dispatchers must be idempotent, because at-least-once delivery means a
//     sink may observe the same delivery more than once.
package notify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Severity mirrors the incident review levels so routing rules stay familiar.
type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
	SeverityNotice   Severity = "notice"
)

// Status is the lifecycle state of a single notification envelope.
type Status string

const (
	// StatusPending means the envelope still has to be (re)tried.
	StatusPending Status = "pending"
	// StatusFailed marks an attempt that errored; another attempt is scheduled.
	StatusFailed Status = "failed"
	// StatusDead means max attempts were exhausted; manual intervention is
	// required (manual retry is still possible).
	StatusDead Status = "dead"
	// StatusSent is terminal success as far as the outbox is concerned.
	StatusSent Status = "sent"
)

// Channel describes how an on-call sink accepts messages.
type Channel string

const (
	ChannelLog     Channel = "log"
	ChannelWebhook Channel = "webhook"
	ChannelNoop    Channel = "noop"
)

// Route is a configured on-call destination selected by chamber and severity.
type Route struct {
	Name     string   `json:"name"`
	Chambers []string `json:"chambers"` // empty means all chambers
	MinLevel Severity `json:"min_level"`
	Channel  Channel  `json:"channel"`
	Target   string   `json:"target"`
	Enabled  bool     `json:"enabled"`
}

// matches reports whether the route applies to an event for chamber/severity.
func (r Route) matches(chamberID string, severity Severity) bool {
	if !r.Enabled {
		return false
	}
	if severityRank(severity) < severityRank(r.MinLevel) {
		return false
	}
	if len(r.Chambers) == 0 {
		return true
	}
	for _, candidate := range r.Chambers {
		if candidate == chamberID {
			return true
		}
	}
	return false
}

func severityRank(level Severity) int {
	switch level {
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

// Envelope is one pending or delivered notification for one (event, route)
// pair. Its ID is deterministic from the originating event and route, which is
// what makes restarts idempotent.
type Envelope struct {
	ID          string          `json:"id"`
	EventID     uuid.UUID       `json:"event_id"`
	Sequence    uint64          `json:"sequence"`
	ChamberID   string          `json:"chamber_id"`
	EventType   model.EventType `json:"event_type"`
	Severity    Severity        `json:"severity"`
	Summary     string          `json:"summary"`
	RouteName   string          `json:"route_name"`
	Channel     Channel         `json:"channel"`
	Target      string          `json:"target"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	NotBefore   time.Time       `json:"not_before"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	SentAt      *time.Time      `json:"sent_at,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	OccurredAt  time.Time       `json:"event_occurred_at"`
	Details     json.RawMessage `json:"details,omitempty"`
	History     []AttemptEntry  `json:"history,omitempty"`
}

// Pending reports whether the pump still has work for this envelope.
func (e Envelope) Pending() bool {
	return e.Status == StatusPending || e.Status == StatusFailed
}

// Due reports whether the envelope may be attempted at now.
func (e Envelope) Due(now time.Time) bool {
	return e.Pending() && !now.Before(e.NotBefore)
}

// deterministicID derives a stable envelope id from the originating event and
// the target route. Re-ingesting the same event after a restart yields the
// exact same id instead of a new notification.
func deterministicID(event model.Event, routeName string) string {
	sum := sha256.Sum256([]byte(event.ID.String() + "|" + routeName))
	return "ntf_" + hex.EncodeToString(sum[:16])
}

// classify maps a persisted event onto its notification meaning. Events outside
// this map are operational telemetry and never page on-call staff.
func classify(kind model.EventType) (Severity, string, bool) {
	switch kind {
	case model.EventEmergencyStarted:
		return SeverityCritical, "应急停止已触发：正在取消闸室活动指令", true
	case model.EventEmergencySettled:
		return SeverityNotice, "应急动作完成：设备已达到稳定状态", true
	case model.EventPlanCancelled:
		return SeverityWarning, "船队计划已取消，信号与队列已补偿", true
	case model.EventReplayWarning:
		return SeverityWarning, "进程恢复后重放了未落快照事件，需要人工复核", true
	default:
		return "", "", false
	}
}

func cleanError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 500 {
		return value[:500] + "…"
	}
	return value
}
