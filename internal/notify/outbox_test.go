package notify

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func decodeBody(r *http.Request, target any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(target)
}

func fixedClock(start time.Time) (func() time.Time, func(time.Duration)) {
	current := start
	var mu sync.Mutex
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	advance := func(delta time.Duration) {
		mu.Lock()
		current = current.Add(delta)
		mu.Unlock()
	}
	return now, advance
}

func testEvent(kind model.EventType) model.Event {
	return model.Event{
		ID: uuid.New(), ChamberID: "chamber-1", Sequence: 1,
		Type: kind, OccurredAt: time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC),
	}
}

func newTestOutbox(t *testing.T, store OutboxStore, routes []Route, dispatcher Dispatcher, now func() time.Time) *Outbox {
	t.Helper()
	registry := NewRouteRegistry()
	for _, route := range routes {
		if err := registry.Upsert(route); err != nil {
			t.Fatalf("route upsert: %v", err)
		}
	}
	config := Config{
		PumpInterval: time.Minute, BaseBackoff: 10 * time.Millisecond,
		MaxBackoff: time.Second, MaxAttempts: 4,
	}
	return NewOutbox(store, registry, map[Channel]Dispatcher{
		ChannelLog: dispatcher, ChannelNoop: NoopDispatcher,
	}, now, config)
}

func dutyRoutes() []Route {
	return []Route{
		{Name: "ch1", Chambers: []string{"chamber-1"}, MinLevel: SeverityWarning, Channel: ChannelLog, Enabled: true},
		{Name: "ch2", Chambers: []string{"chamber-2"}, MinLevel: SeverityWarning, Channel: ChannelLog, Enabled: true},
		{Name: "critical-all", MinLevel: SeverityCritical, Channel: ChannelLog, Enabled: true},
	}
}

type capturingDispatcher struct {
	mu       sync.Mutex
	messages []Message
	fail     bool
}

func (d *capturingDispatcher) Dispatch(_ context.Context, message Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.messages = append(d.messages, message)
	if d.fail {
		return errors.New("sink unavailable")
	}
	return nil
}

func (d *capturingDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.messages)
}

// Repeated ingestion of the same event must never mint a second notification,
// which is the restart-safety guarantee.
func TestIngestIsIdempotentForSameEvent(t *testing.T) {
	now, _ := fixedClock(time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	dispatcher := &capturingDispatcher{}
	out := newTestOutbox(t, NewMemoryOutbox(), dutyRoutes(), dispatcher, now)
	ctx := context.Background()

	event := testEvent(model.EventEmergencyStarted)
	if err := out.Ingest(ctx, event); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := out.Ingest(ctx, event); err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	pending, err := out.Pending(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	// chamber-1 duty route + global critical escalation; chamber-2 excluded.
	if len(pending) != 2 {
		t.Fatalf("expected 2 envelopes, got %d", len(pending))
	}
	wantID := deterministicID(event, "ch1")
	var ch1Envelope *Envelope
	for index := range pending {
		if pending[index].RouteName == "ch1" {
			ch1Envelope = &pending[index]
		}
	}
	if ch1Envelope == nil {
		t.Fatalf("chamber-1 duty envelope missing")
	}
	if ch1Envelope.ID != wantID {
		t.Fatalf("envelope id not deterministic: got %s want %s", ch1Envelope.ID, wantID)
	}
	for _, envelope := range pending {
		if envelope.EventID != event.ID {
			t.Fatalf("origin event id missing: %v", envelope)
		}
	}
}

func TestRoutingFiltersChamberAndSeverity(t *testing.T) {
	now, _ := fixedClock(time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	out := newTestOutbox(t, NewMemoryOutbox(), dutyRoutes(), &capturingDispatcher{}, now)
	ctx := context.Background()

	ch2Critical := testEvent(model.EventEmergencyStarted)
	ch2Critical.ChamberID = "chamber-2"
	if err := out.Ingest(ctx, ch2Critical); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	// Notice-level event on chamber-1 only matches warning routes? It must
	// match nothing because min level is warning.
	settled := testEvent(model.EventEmergencySettled)
	if err := out.Ingest(ctx, settled); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	pending, _ := out.Pending(ctx)
	// ch2 duty + critical escalation for the critical event = 2; settled = 0.
	if len(pending) != 2 {
		t.Fatalf("expected 2 routed envelopes, got %d: %+v", len(pending), pending)
	}
	for _, envelope := range pending {
		if envelope.RouteName == "ch1" {
			t.Fatalf("chamber-2 event routed to chamber-1 sink")
		}
	}

	// Irrelevant event types never page anyone.
	before := len(pending)
	if err := out.Ingest(ctx, testEvent(model.EventCycleAdvanced)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	pending, _ = out.Pending(ctx)
	if len(pending) != before {
		t.Fatalf("operational event produced a notification")
	}
}

func TestFailureRetriesBackoffDeadLettersAndManualRetry(t *testing.T) {
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	now, advance := fixedClock(start)
	dispatcher := &capturingDispatcher{fail: true}
	routes := []Route{{Name: "ch1", Chambers: []string{"chamber-1"}, MinLevel: SeverityWarning, Channel: ChannelLog, Enabled: true}}
	out := newTestOutbox(t, NewMemoryOutbox(), routes, dispatcher, now)
	ctx := context.Background()

	event := testEvent(model.EventPlanCancelled)
	if err := out.Ingest(ctx, event); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := out.processDue(ctx); err != nil {
		t.Fatalf("processDue: %v", err)
	}
	pending, _ := out.Pending(ctx)
	if len(pending) != 1 || pending[0].Status != StatusFailed || pending[0].Attempts != 1 {
		t.Fatalf("expected one failed attempt, got %+v", pending)
	}
	if !pending[0].NotBefore.After(start) {
		t.Fatalf("backoff not scheduled: %v", pending[0].NotBefore)
	}

	// Before backoff elapses nothing is retried.
	if err := out.processDue(ctx); err != nil {
		t.Fatalf("processDue: %v", err)
	}
	pending, _ = out.Pending(ctx)
	if pending[0].Attempts != 1 {
		t.Fatalf("envelope retried before backoff: attempts=%d", pending[0].Attempts)
	}

	// Exhaust attempts → dead letter.
	advance(time.Hour)
	for i := 0; i < 5; i++ {
		if err := out.processDue(ctx); err != nil {
			t.Fatalf("processDue: %v", err)
		}
		advance(time.Hour)
	}
	pending, _ = out.Pending(ctx)
	if pending[0].Status != StatusDead || pending[0].Attempts != 4 {
		t.Fatalf("expected dead after 4 attempts, got %+v", pending[0])
	}
	if len(pending[0].History) != 4 {
		t.Fatalf("expected 4 history entries, got %d", len(pending[0].History))
	}

	// Manual retry reopens immediately; once the sink recovers it goes sent.
	dispatcher.fail = false
	if _, err := out.Retry(ctx, pending[0].ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	advance(time.Hour)
	if err := out.processDue(ctx); err != nil {
		t.Fatalf("processDue: %v", err)
	}
	deliveries, err := out.Deliveries(ctx)
	if err != nil {
		t.Fatalf("deliveries: %v", err)
	}
	if deliveries[0].Status != StatusSent {
		t.Fatalf("expected sent after recovery, got %s", deliveries[0].Status)
	}
	if deliveries[0].Attempts != 1 {
		t.Fatalf("manual retry should reset attempt budget, got attempts=%d", deliveries[0].Attempts)
	}
	foundManual := false
	for _, entry := range deliveries[0].History {
		if entry.Manual {
			foundManual = true
		}
	}
	if !foundManual {
		t.Fatalf("manual retry not recorded in history: %+v", deliveries[0].History)
	}
}

// A simulated restart must reload the outbox and reproduce identical envelope
// identity — no new, different notification for the same origin event.
func TestRestartReproducesSameNotification(t *testing.T) {
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	now, advance := fixedClock(start)
	dispatcher := &capturingDispatcher{fail: true}
	routes := []Route{{Name: "ch1", Chambers: []string{"chamber-1"}, MinLevel: SeverityWarning, Channel: ChannelLog, Enabled: true}}
	root := filepath.Join(t.TempDir(), "data")

	first := newTestOutbox(t, mustFileStore(t, root), routes, dispatcher, now)
	ctx := context.Background()
	event := testEvent(model.EventEmergencyStarted)
	if err := first.Ingest(ctx, event); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := first.processDue(ctx); err != nil {
		t.Fatalf("processDue: %v", err)
	}
	firstID := deterministicID(event, "ch1")

	// Process restarts: a brand-new outbox loads the durable snapshot.
	second := newTestOutbox(t, mustFileStore(t, root), routes, dispatcher, now)
	if err := second.Ingest(ctx, event); err != nil {
		t.Fatalf("re-ingest after restart: %v", err)
	}
	pending, _ := second.Pending(ctx)
	if len(pending) != 1 {
		t.Fatalf("expected exactly one envelope after restart, got %d", len(pending))
	}
	if pending[0].ID != firstID {
		t.Fatalf("notification id changed across restart: %s vs %s", pending[0].ID, firstID)
	}
	if pending[0].Attempts != 1 || pending[0].Status != StatusFailed {
		t.Fatalf("attempt state lost across restart: %+v", pending[0])
	}

	// Event eventually delivers after the sink recovers.
	dispatcher.fail = false
	advance(time.Hour)
	if err := second.processDue(ctx); err != nil {
		t.Fatalf("processDue: %v", err)
	}
	deliveries, _ := second.Deliveries(ctx)
	if deliveries[0].Status != StatusSent || deliveries[0].EventID != event.ID {
		t.Fatalf("unexpected delivery after restart: %+v", deliveries[0])
	}
}

func mustFileStore(t *testing.T, root string) OutboxStore {
	t.Helper()
	store, err := NewFileOutbox(filepath.Join(root, "notify"))
	if err != nil {
		t.Fatalf("file outbox: %v", err)
	}
	return store
}

func TestWebhookCarriesOriginEventAndIdempotencyKey(t *testing.T) {
	var mu sync.Mutex
	received := make([]Message, 0, 2)
	keys := make([]string, 0, 2)
	statuses := []int{http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var message Message
		if err := decodeBody(r, &message); err != nil {
			t.Errorf("decode: %v", err)
		}
		mu.Lock()
		index := len(received)
		received = append(received, message)
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		code := statuses[index]
		mu.Unlock()
		w.WriteHeader(code)
	}))
	defer server.Close()

	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	now, advance := fixedClock(start)
	registry := NewRouteRegistry()
	if err := registry.Upsert(Route{
		Name: "hook", MinLevel: SeverityWarning,
		Channel: ChannelWebhook, Target: server.URL, Enabled: true,
	}); err != nil {
		t.Fatalf("route: %v", err)
	}
	out := NewOutbox(NewMemoryOutbox(), registry, map[Channel]Dispatcher{
		ChannelWebhook: NewWebhookDispatcher(server.Client()),
	}, now, Config{
		PumpInterval: time.Minute, BaseBackoff: time.Millisecond,
		MaxBackoff: time.Millisecond, MaxAttempts: 5,
	})
	ctx := context.Background()
	event := testEvent(model.EventPlanCancelled)
	if err := out.Ingest(ctx, event); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := out.processDue(ctx); err != nil {
			t.Fatalf("processDue: %v", err)
		}
		advance(time.Second)
	}
	deliveries, _ := out.Deliveries(ctx)
	if deliveries[0].Status != StatusSent {
		t.Fatalf("expected webhook success on third try, got %s (%s)", deliveries[0].Status, deliveries[0].LastError)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("expected 3 webhook calls, got %d", len(received))
	}
	if received[0].OriginEventID != event.ID.String() {
		t.Fatalf("origin event id not delivered: %s", received[0].OriginEventID)
	}
	if keys[0] != keys[1] || keys[0] != deliveries[0].ID {
		t.Fatalf("idempotency key not stable across retries: %v", keys)
	}
}
