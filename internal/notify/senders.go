package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// webhookPayload is the JSON body posted to webhook duty receivers. The source
// event id is always included so downstream systems can dedupe exactly as we do.
type webhookPayload struct {
	NotificationID string          `json:"notification_id"`
	SourceEventID  string          `json:"source_event_id"`
	ChamberID      string          `json:"chamber_id"`
	EventType      string          `json:"event_type"`
	EventSequence  uint64          `json:"event_sequence"`
	Severity       Severity        `json:"severity"`
	Title          string          `json:"title"`
	Summary        string          `json:"summary"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Source         json.RawMessage `json:"source,omitempty"`
}

// LogSender records notifications through structured logging and always succeeds;
// it is the safe default transport for local duty desks.
type LogSender struct {
	logger *slog.Logger
}

func NewLogSender(logger *slog.Logger) *LogSender {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogSender{logger: logger}
}

func (s *LogSender) Send(ctx context.Context, notification Notification, receiver Receiver) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.logger.Info("duty notification",
		"receiver", receiver.ID, "severity", notification.Severity,
		"chamber", notification.ChamberID, "title", notification.Title,
		"summary", notification.Summary, "source_event", notification.SourceEventID)
	return nil
}

// WebhookSender posts the notification and treats any 2xx response as delivered.
// It never panics and never mutates domain state; the caller owns retry policy.
type WebhookSender struct {
	client *http.Client
}

func NewWebhookSender(client *http.Client) *WebhookSender {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &WebhookSender{client: client}
}

func (s *WebhookSender) Send(ctx context.Context, notification Notification, receiver Receiver) error {
	payload := webhookPayload{
		NotificationID: notification.ID.String(),
		SourceEventID:  notification.SourceEventID.String(),
		ChamberID:      notification.ChamberID,
		EventType:      string(notification.EventType),
		EventSequence:  notification.EventSequence,
		Severity:       notification.Severity,
		Title:          notification.Title,
		Summary:        notification.Summary,
		OccurredAt:     notification.OccurredAt,
		Source:         notification.SourcePayload,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, receiver.Target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("X-LockFlow-Event", notification.SourceEventID.String())
	request.Header.Set("X-LockFlow-Notification", notification.ID.String())
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		text := strings.TrimSpace(string(detail))
		if text == "" {
			text = http.StatusText(response.StatusCode)
		}
		return fmt.Errorf("webhook returned %d: %s", response.StatusCode, text)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return nil
}

// routingSender selects the transport for the receiver kind.
type routingSender struct {
	log     *LogSender
	webhook *WebhookSender
}

func NewRoutingSender(logger *slog.Logger, client *http.Client) Sender {
	return &routingSender{log: NewLogSender(logger), webhook: NewWebhookSender(client)}
}

func (s *routingSender) Send(ctx context.Context, notification Notification, receiver Receiver) error {
	switch receiver.Kind {
	case ReceiverWebhook:
		return s.webhook.Send(ctx, notification, receiver)
	default:
		return s.log.Send(ctx, notification, receiver)
	}
}
