package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

const (
	defaultMaxAttempts = 5
	// backoffBase doubles with every failed attempt and is capped at backoffCap.
	backoffBase = 5 * time.Second
	backoffCap  = 15 * time.Minute
)

// EventSource is the read boundary over the domain event journal.
type EventSource interface {
	Chambers(ctx context.Context) ([]string, error)
	Events(ctx context.Context, chamberID string, after uint64) ([]model.Event, error)
}

// Sender delivers one materialized notification to one receiver.
type Sender interface {
	Send(ctx context.Context, notification Notification, receiver Receiver) error
}

// Outbox projects safety events into deterministic notifications and drives their
// delivery. Every notification id is derived from (source event, receiver), so
// restarting or rescanning the journal never creates a different notification.
// Delivery runs outside the safety transaction: a failed send only leaves a
// pending record and can never roll back an emergency stop or cancellation.
type Outbox struct {
	mu       sync.RWMutex
	store    OutboxStore
	roster   *Roster
	source   EventSource
	sender   Sender
	logger   *slog.Logger
	now      func() time.Time
	items    map[uuid.UUID]Notification
	attempts []DeliveryAttempt
	cursors  map[string]uint64
	// rosterVersion is the roster config version the cursors are valid for.
	rosterVersion uint64
}

func NewOutbox(store OutboxStore, roster *Roster, source EventSource, sender Sender, logger *slog.Logger, now func() time.Time) *Outbox {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Outbox{
		store: store, roster: roster, source: source, sender: sender,
		logger: logger, now: now,
		items:   make(map[uuid.UUID]Notification),
		cursors: make(map[string]uint64),
	}
}

// Load restores previously materialized notifications and delivery records.
func (o *Outbox) Load(ctx context.Context) error {
	items, attempts, err := o.store.LoadState(ctx)
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.items = items
	o.attempts = attempts
	o.mu.Unlock()
	return nil
}

// ProjectEvents scans the journal and materializes one notification per matching
// (event, receiver) pair. It is idempotent: deterministic ids make rescans no-ops.
// A per-chamber cursor limits rescans; when roster configuration changes the scan
// restarts from zero so new receivers see pre-existing events.
func (o *Outbox) ProjectEvents(ctx context.Context) (int, error) {
	if o.source == nil {
		return 0, errors.New("event source is not bound")
	}
	chambers, err := o.source.Chambers(ctx)
	if err != nil {
		return 0, err
	}
	o.mu.Lock()
	if o.rosterVersion != o.roster.Version() {
		o.cursors = make(map[string]uint64)
		o.rosterVersion = o.roster.Version()
	}
	o.mu.Unlock()
	var created []Notification
	for _, chamberID := range chambers {
		if err := ctx.Err(); err != nil {
			return len(created), err
		}
		o.mu.RLock()
		after := o.cursors[chamberID]
		o.mu.RUnlock()
		events, err := o.source.Events(ctx, chamberID, after)
		if err != nil {
			return len(created), err
		}
		var maxSequence uint64 = after
		for _, event := range events {
			if event.Sequence <= maxSequence {
				continue
			}
			maxSequence = event.Sequence
			meaning, ok := classifyEvent(event)
			if !ok {
				continue
			}
			for _, receiver := range o.roster.Route(event.ChamberID, meaning.severity) {
				notification := Notification{
					ID:            DeriveID(event.ID, receiver.ID),
					SourceEventID: event.ID,
					ChamberID:     event.ChamberID,
					EventType:     event.Type,
					EventSequence: event.Sequence,
					Severity:      meaning.severity,
					Title:         meaning.title,
					Summary:       meaning.summary,
					ReceiverID:    receiver.ID,
					Status:        StatusPending,
					MaxAttempts:   defaultMaxAttempts,
					CreatedAt:     o.now().UTC(),
					OccurredAt:    event.OccurredAt.UTC(),
					NextAttemptAt: o.now().UTC(),
					SourcePayload: event.Payload,
				}
				o.mu.Lock()
				if _, exists := o.items[notification.ID]; !exists {
					o.items[notification.ID] = notification
					created = append(created, notification)
				}
				o.mu.Unlock()
			}
		}
		o.mu.Lock()
		if o.cursors[chamberID] < maxSequence {
			o.cursors[chamberID] = maxSequence
		}
		o.mu.Unlock()
	}
	if len(created) > 0 {
		o.mu.RLock()
		snapshot := sortedNotifications(o.items)
		o.mu.RUnlock()
		if err := o.store.SaveNotifications(ctx, snapshot); err != nil {
			return 0, err
		}
	}
	return len(created), nil
}

// DispatchDue attempts delivery of every notification whose backoff has elapsed.
func (o *Outbox) DispatchDue(ctx context.Context) error {
	o.mu.RLock()
	due := make([]Notification, 0)
	now := o.now().UTC()
	for _, item := range o.items {
		if item.Status == StatusPending && !item.NextAttemptAt.After(now) {
			due = append(due, item)
		}
	}
	o.mu.RUnlock()
	sort.Slice(due, func(i, j int) bool { return due[i].NextAttemptAt.Before(due[j].NextAttemptAt) })
	for _, item := range due {
		if err := ctx.Err(); err != nil {
			return err
		}
		o.attemptOne(ctx, item.ID)
	}
	return nil
}

func (o *Outbox) attemptOne(ctx context.Context, id uuid.UUID) {
	o.mu.RLock()
	item, ok := o.items[id]
	o.mu.RUnlock()
	if !ok || item.Status != StatusPending {
		return
	}
	receiver, configured := o.roster.Get(item.ReceiverID)
	item.Attempts++
	started := o.now().UTC()

	var sendErr error
	if !configured {
		sendErr = errors.New("duty receiver is no longer configured")
	} else {
		sendErr = o.sender.Send(ctx, item, receiver)
	}

	finished := o.now().UTC()
	record := DeliveryAttempt{
		NotificationID: item.ID, Attempt: item.Attempts,
		StartedAt: started, DurationMS: finished.Sub(started).Milliseconds(),
	}
	if sendErr == nil {
		item.Status = StatusDelivered
		item.DeliveredAt = &finished
		item.NextAttemptAt = time.Time{}
		item.LastError = ""
		record.Status = string(StatusDelivered)
	} else {
		item.LastError = sendErr.Error()
		record.Status = "failed"
		record.Detail = sendErr.Error()
		if item.Attempts >= item.MaxAttempts {
			item.Status = StatusFailed
			item.NextAttemptAt = time.Time{}
		} else {
			item.NextAttemptAt = finished.Add(backoff(item.Attempts))
		}
		o.logger.Warn("notification delivery failed; safety action remains in force",
			"notification", item.ID, "source_event", item.SourceEventID,
			"chamber", item.ChamberID, "attempt", item.Attempts, "error", sendErr)
	}

	// The attempt record is appended first so a crash never hides a send that
	// happened, even if the outbox snapshot rewrite is interrupted afterwards.
	if err := o.store.AppendAttempt(context.WithoutCancel(ctx), record); err != nil {
		o.logger.Error("persist delivery attempt failed", "notification", item.ID, "error", err)
	}
	o.mu.Lock()
	o.attempts = append(o.attempts, record)
	o.items[item.ID] = item
	snapshot := sortedNotifications(o.items)
	o.mu.Unlock()
	if err := o.store.SaveNotifications(context.WithoutCancel(ctx), snapshot); err != nil {
		o.logger.Error("persist notification state failed", "notification", item.ID, "error", err)
	}
}

// backoff returns the wait before retry number (1-based, already failed).
func backoff(failedAttempts int) time.Duration {
	wait := backoffBase
	for shift := 1; shift < failedAttempts && wait < backoffCap; shift++ {
		wait *= 2
	}
	if wait > backoffCap {
		wait = backoffCap
	}
	return wait
}

// Retry requeues an exhausted (failed) notification for another attempt.
func (o *Outbox) Retry(ctx context.Context, id uuid.UUID) (Notification, error) {
	o.mu.Lock()
	item, ok := o.items[id]
	if !ok {
		o.mu.Unlock()
		return Notification{}, errors.New("notification not found")
	}
	if item.Status != StatusFailed {
		o.mu.Unlock()
		return Notification{}, fmt.Errorf("only failed notifications can be retried, status=%s", item.Status)
	}
	item.Status = StatusPending
	item.NextAttemptAt = o.now().UTC()
	item.LastError = ""
	o.items[id] = item
	snapshot := sortedNotifications(o.items)
	o.mu.Unlock()
	if err := o.store.SaveNotifications(ctx, snapshot); err != nil {
		return Notification{}, err
	}
	return item, nil
}

// Run keeps notifications projected and delivered until ctx is cancelled.
func (o *Outbox) Run(ctx context.Context, interval time.Duration) {
	o.sweep(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.sweep(ctx)
		}
	}
}

func (o *Outbox) sweep(ctx context.Context) {
	if created, err := o.ProjectEvents(ctx); err != nil {
		o.logger.Error("project notification events failed", "error", err)
	} else if created > 0 {
		o.logger.Info("notifications materialized from safety events", "count", created)
	}
	if err := o.DispatchDue(ctx); err != nil && !errors.Is(err, context.Canceled) {
		o.logger.Error("dispatch pending notifications failed", "error", err)
	}
}

// Pending returns notifications still awaiting delivery, most urgent first.
func (o *Outbox) Pending() []Notification {
	o.mu.RLock()
	result := make([]Notification, 0)
	for _, item := range o.items {
		if item.Status == StatusPending {
			result = append(result, item)
		}
	}
	o.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].Severity != result[j].Severity {
			return result[i].Severity.Rank() > result[j].Severity.Rank()
		}
		return result[i].NextAttemptAt.Before(result[j].NextAttemptAt)
	})
	return result
}

// Notifications returns the full outbox with optional chamber/status filters.
func (o *Outbox) Notifications(chamberID string, status NotificationStatus) []Notification {
	o.mu.RLock()
	result := make([]Notification, 0, len(o.items))
	for _, item := range o.items {
		if chamberID != "" && item.ChamberID != chamberID {
			continue
		}
		if status != "" && item.Status != status {
			continue
		}
		result = append(result, item)
	}
	o.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result
}

func (o *Outbox) Get(id uuid.UUID) (Notification, bool) {
	o.mu.RLock()
	item, ok := o.items[id]
	o.mu.RUnlock()
	return item, ok
}

// Records returns delivery attempts joined with notification and receiver context,
// newest first. A zero or negative limit returns all records.
func (o *Outbox) Records(limit int) []DeliveryRecord {
	o.mu.RLock()
	attempts := append([]DeliveryAttempt(nil), o.attempts...)
	items := make(map[uuid.UUID]Notification, len(o.items))
	for id, item := range o.items {
		items[id] = item
	}
	o.mu.RUnlock()
	sort.Slice(attempts, func(i, j int) bool {
		if !attempts[i].StartedAt.Equal(attempts[j].StartedAt) {
			return attempts[i].StartedAt.After(attempts[j].StartedAt)
		}
		return attempts[i].NotificationID.String() > attempts[j].NotificationID.String()
	})
	if limit > 0 && len(attempts) > limit {
		attempts = attempts[:limit]
	}
	records := make([]DeliveryRecord, 0, len(attempts))
	for _, attempt := range attempts {
		record := DeliveryRecord{DeliveryAttempt: attempt}
		if item, ok := items[attempt.NotificationID]; ok {
			record.SourceEventID = item.SourceEventID
			record.ChamberID = item.ChamberID
			record.Severity = item.Severity
			record.Title = item.Title
			record.ReceiverID = item.ReceiverID
			record.EventType = item.EventType
		}
		if receiver, ok := o.roster.Get(record.ReceiverID); ok {
			record.ReceiverName = receiver.Name
		}
		records = append(records, record)
	}
	return records
}
