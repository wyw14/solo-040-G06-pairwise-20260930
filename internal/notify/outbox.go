package notify

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-96/internal/model"
)

// defaultMaxAttempts bounds automatic retries before an envelope dead-letters.
const defaultMaxAttempts = 8

// AttemptEntry records one delivery try; the attempt log travels with the
// envelope and powers the delivery-records view.
type AttemptEntry struct {
	Attempt int       `json:"attempt"`
	Status  Status    `json:"status"`
	Error   string    `json:"error,omitempty"`
	At      time.Time `json:"at"`
	Manual  bool      `json:"manual,omitempty"`
}

// Config tunes pump timing and retry behaviour.
type Config struct {
	PumpInterval time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	MaxAttempts  int
}

func defaultConfig() Config {
	return Config{
		PumpInterval: time.Second,
		BaseBackoff:  5 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  defaultMaxAttempts,
	}
}

// Outbox converts events into envelopes and drives delivery. It never returns
// delivery errors to event producers: safety actions commit independently in
// the journal and notifications catch up asynchronously.
type Outbox struct {
	store       OutboxStore
	routes      *RouteRegistry
	dispatchers map[Channel]Dispatcher
	now         func() time.Time
	config      Config
	backoff     func(attempt int, now time.Time) time.Time

	mu        sync.Mutex
	envelopes map[string]Envelope
	loaded    bool
	kick      chan struct{}
}

func NewOutbox(store OutboxStore, routes *RouteRegistry, dispatchers map[Channel]Dispatcher, now func() time.Time, config Config) *Outbox {
	if now == nil {
		now = time.Now
	}
	if config.PumpInterval <= 0 {
		config = defaultConfig()
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = defaultMaxAttempts
	}
	if routes == nil {
		routes = NewRouteRegistry()
	}
	if dispatchers == nil {
		dispatchers = map[Channel]Dispatcher{}
	}
	if _, ok := dispatchers[ChannelLog]; !ok {
		dispatchers[ChannelLog] = NewLogDispatcher(nil)
	}
	if _, ok := dispatchers[ChannelNoop]; !ok {
		dispatchers[ChannelNoop] = NoopDispatcher
	}
	out := &Outbox{
		store: store, routes: routes, dispatchers: dispatchers,
		now: now, config: config,
		envelopes: make(map[string]Envelope),
		kick:      make(chan struct{}, 1),
	}
	out.backoff = out.defaultBackoff
	return out
}

func (o *Outbox) defaultBackoff(attempt int, now time.Time) time.Time {
	delay := o.config.BaseBackoff
	for index := 1; index < attempt; index++ {
		if delay >= o.config.MaxBackoff/2 {
			delay = o.config.MaxBackoff
			break
		}
		delay *= 2
	}
	if delay > o.config.MaxBackoff {
		delay = o.config.MaxBackoff
	}
	return now.Add(delay)
}

// ensureLoaded reads the durable snapshot once before any mutation or pump run.
func (o *Outbox) ensureLoaded(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	stored, err := o.store.Load(ctx)
	if err != nil {
		return err
	}
	for _, envelope := range stored {
		o.envelopes[envelope.ID] = envelope
	}
	o.loaded = true
	return nil
}

// Ingest turns one persisted event into one envelope per matching on-call
// route. Re-ingesting the same event is a no-op: the deterministic envelope id
// already exists, so a process restart never creates a second, different
// notification for the same event.
func (o *Outbox) Ingest(ctx context.Context, event model.Event) error {
	severity, summary, relevant := classify(event.Type)
	if !relevant {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ensureLoadedLocked(ctx); err != nil {
		return err
	}
	now := o.now().UTC()
	created := false
	for _, route := range o.routes.targets(event.ChamberID, severity) {
		id := deterministicID(event, route.Name)
		if _, exists := o.envelopes[id]; exists {
			continue
		}
		envelope := Envelope{
			ID: id, EventID: event.ID, Sequence: event.Sequence,
			ChamberID: event.ChamberID, EventType: event.Type,
			Severity: severity, Summary: summary,
			RouteName: route.Name, Channel: route.Channel, Target: route.Target,
			Status: StatusPending, MaxAttempts: o.config.MaxAttempts,
			NotBefore: now, CreatedAt: now, UpdatedAt: now,
			OccurredAt: event.OccurredAt, Details: append(json.RawMessage(nil), event.Payload...),
		}
		o.envelopes[id] = envelope
		created = true
	}
	if !created {
		return nil
	}
	if err := o.persistLocked(ctx); err != nil {
		return err
	}
	o.nudge()
	return nil
}

// Run pumps the outbox until the context is cancelled.
func (o *Outbox) Run(ctx context.Context) error {
	o.mu.Lock()
	if err := o.ensureLoadedLocked(ctx); err != nil {
		o.mu.Unlock()
		return err
	}
	o.mu.Unlock()

	ticker := time.NewTicker(o.config.PumpInterval)
	defer ticker.Stop()
	for {
		if err := o.processDue(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "notification pump cycle failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-o.kick:
		}
	}
}

// processDue attempts every envelope whose backoff has elapsed.
func (o *Outbox) processDue(ctx context.Context) error {
	due := o.dueEnvelopes()
	if len(due) == 0 {
		return nil
	}
	for _, envelope := range due {
		if err := ctx.Err(); err != nil {
			break
		}
		dispatcher, ok := o.dispatchers[envelope.Channel]
		var attemptErr error
		if !ok {
			attemptErr = errors.New("no dispatcher configured for channel " + string(envelope.Channel))
		} else {
			attemptErr = dispatcher.Dispatch(ctx, o.message(envelope, envelope.Attempts+1))
		}
		o.recordResult(envelope.ID, attemptErr)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	// Attempt outcomes must survive even when the request context is closing.
	return o.persistLocked(context.WithoutCancel(ctx))
}

func (o *Outbox) dueEnvelopes() []Envelope {
	now := o.now()
	o.mu.Lock()
	defer o.mu.Unlock()
	var due []Envelope
	for _, envelope := range o.envelopes {
		if envelope.Due(now) {
			due = append(due, envelope)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].NotBefore.Equal(due[j].NotBefore) {
			return due[i].NotBefore.Before(due[j].NotBefore)
		}
		return due[i].ID < due[j].ID
	})
	return due
}

// recordResult applies one attempt outcome under the lock. Delivery failure
// only reschedules the envelope — it is never surfaced to the safety path.
func (o *Outbox) recordResult(id string, attemptErr error) {
	now := o.now().UTC()
	o.mu.Lock()
	defer o.mu.Unlock()
	envelope := o.envelopes[id]
	envelope.Attempts++
	entry := AttemptEntry{Attempt: envelope.Attempts, At: now}
	if attemptErr == nil {
		envelope.Status = StatusSent
		entry.Status = StatusSent
		envelope.LastError = ""
		envelope.NotBefore = now
		envelope.SentAt = &now
	} else {
		envelope.LastError = cleanError(attemptErr.Error())
		entry.Error = envelope.LastError
		if envelope.Attempts >= envelope.MaxAttempts {
			envelope.Status = StatusDead
			entry.Status = StatusDead
		} else {
			envelope.Status = StatusFailed
			entry.Status = StatusFailed
			envelope.NotBefore = o.backoff(envelope.Attempts+1, now)
		}
	}
	envelope.UpdatedAt = now
	envelope.History = append(envelope.History, entry)
	if len(envelope.History) > 20 {
		envelope.History = envelope.History[len(envelope.History)-20:]
	}
	o.envelopes[id] = envelope
}

func (o *Outbox) message(envelope Envelope, attempt int) Message {
	return Message{
		ID: envelope.ID, OriginEventID: envelope.EventID.String(),
		Sequence: envelope.Sequence, ChamberID: envelope.ChamberID,
		EventType: string(envelope.EventType), Severity: envelope.Severity,
		Summary: envelope.Summary, RouteName: envelope.RouteName,
		Target: envelope.Target, Attempt: attempt,
		OccurredAt: envelope.OccurredAt, Details: envelope.Details,
	}
}

// Retry reopens a failed or dead envelope immediately with a fresh attempt
// budget. It is the manual escape hatch surfaced by the pending view.
func (o *Outbox) Retry(ctx context.Context, id string) (Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ensureLoadedLocked(ctx); err != nil {
		return Envelope{}, err
	}
	envelope, ok := o.envelopes[id]
	if !ok {
		return Envelope{}, ErrEnvelopeNotFound
	}
	if envelope.Status == StatusSent {
		return envelope, nil
	}
	now := o.now().UTC()
	envelope.Status = StatusPending
	envelope.Attempts = 0
	envelope.LastError = ""
	envelope.NotBefore = now
	envelope.UpdatedAt = now
	envelope.History = append(envelope.History, AttemptEntry{
		Attempt: envelope.Attempts, Status: StatusPending, At: now, Manual: true,
	})
	o.envelopes[id] = envelope
	if err := o.persistLocked(ctx); err != nil {
		return Envelope{}, err
	}
	o.nudge()
	return envelope, nil
}

// Pending returns envelopes still awaiting delivery (including dead-lettered).
func (o *Outbox) Pending(ctx context.Context) ([]Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ensureLoadedLocked(ctx); err != nil {
		return nil, err
	}
	var result []Envelope
	for _, envelope := range o.envelopes {
		if envelope.Status != StatusSent {
			result = append(result, envelope)
		}
	}
	sortEnvelopes(result)
	return result, nil
}

// Deliveries returns every envelope with its attempt history, newest first.
func (o *Outbox) Deliveries(ctx context.Context) ([]Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ensureLoadedLocked(ctx); err != nil {
		return nil, err
	}
	result := make([]Envelope, 0, len(o.envelopes))
	for _, envelope := range o.envelopes {
		result = append(result, envelope)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].UpdatedAt.After(result[j].UpdatedAt)
	})
	return result, nil
}

func sortEnvelopes(values []Envelope) {
	sort.Slice(values, func(i, j int) bool {
		if !values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].CreatedAt.Before(values[j].CreatedAt)
		}
		return values[i].ID < values[j].ID
	})
}

// nudge wakes the pump after an out-of-band state change.
func (o *Outbox) nudge() {
	select {
	case o.kick <- struct{}{}:
	default:
	}
}

func (o *Outbox) ensureLoadedLocked(ctx context.Context) error {
	if o.loaded {
		return nil
	}
	stored, err := o.store.Load(ctx)
	if err != nil {
		return err
	}
	for _, envelope := range stored {
		o.envelopes[envelope.ID] = envelope
	}
	o.loaded = true
	return nil
}

func (o *Outbox) persistLocked(ctx context.Context) error {
	return o.store.Save(ctx, orderedEnvelopes(o.envelopes))
}

// ErrEnvelopeNotFound indicates a retry targeted an unknown notification.
var ErrEnvelopeNotFound = errors.New("notification not found")
