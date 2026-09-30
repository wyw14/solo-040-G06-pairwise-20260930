package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// Message is the dispatchable payload rendered from an envelope. The
// OriginEventID lets sinks deduplicate retried deliveries.
type Message struct {
	ID            string          `json:"notification_id"`
	OriginEventID string          `json:"origin_event_id"`
	Sequence      uint64          `json:"sequence"`
	ChamberID     string          `json:"chamber_id"`
	EventType     string          `json:"event_type"`
	Severity      Severity        `json:"severity"`
	Summary       string          `json:"summary"`
	RouteName     string          `json:"route_name"`
	Target        string          `json:"-"`
	Attempt       int             `json:"attempt"`
	OccurredAt    time.Time       `json:"event_occurred_at"`
	Details       json.RawMessage `json:"details,omitempty"`
}

// Dispatcher sends a message through one concrete channel. Implementations
// must treat the call as idempotent: the outbox retries, so the same message
// id may arrive more than once.
type Dispatcher interface {
	Dispatch(ctx context.Context, message Message) error
}

// DispatcherFunc adapts plain functions.
type DispatcherFunc func(ctx context.Context, message Message) error

func (f DispatcherFunc) Dispatch(ctx context.Context, message Message) error {
	return f(ctx, message)
}

// LogDispatcher records notifications through slog; it never fails, which
// makes it a safe default on-call channel for deployments without a webhook.
type LogDispatcher struct {
	logger *slog.Logger
}

func NewLogDispatcher(logger *slog.Logger) *LogDispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogDispatcher{logger: logger}
}

func (d *LogDispatcher) Dispatch(_ context.Context, message Message) error {
	d.logger.Info("duty notification",
		"notification_id", message.ID,
		"origin_event_id", message.OriginEventID,
		"chamber", message.ChamberID,
		"severity", string(message.Severity),
		"event", message.EventType,
		"attempt", message.Attempt,
		"summary", message.Summary,
	)
	return nil
}

// NoopDispatcher succeeds without doing anything and is used for disabled or
// testing targets.
var NoopDispatcher Dispatcher = DispatcherFunc(func(context.Context, Message) error { return nil })

// WebhookDispatcher POSTs the message as JSON to an HTTP endpoint. Non-2xx
// responses are retryable errors.
type WebhookDispatcher struct {
	client  *http.Client
	nowFunc func() time.Time
}

func NewWebhookDispatcher(client *http.Client) *WebhookDispatcher {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &WebhookDispatcher{client: client}
}

func (d *WebhookDispatcher) Dispatch(ctx context.Context, message Message) error {
	if message.Target == "" {
		return errors.New("webhook target is missing from the envelope")
	}
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, message.Target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	// Stable idempotency key: deterministic across retries and restarts.
	request.Header.Set("Idempotency-Key", message.ID)
	request.Header.Set("X-Origin-Event-ID", message.OriginEventID)
	response, err := d.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	snippet, _ := io.ReadAll(io.LimitReader(response.Body, 512))
	return fmt.Errorf("webhook responded %s: %s", response.Status, bytes.TrimSpace(snippet))
}
