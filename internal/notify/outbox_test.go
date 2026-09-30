package notify

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func decodeJSONBody(r *http.Request, target any) error {
	defer r.Body.Close()
	return json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(target)
}

type fakeSource struct {
	chambers []string
	events   map[string][]model.Event
}

func (s *fakeSource) Chambers(context.Context) ([]string, error) { return s.chambers, nil }
func (s *fakeSource) Events(_ context.Context, chamberID string, after uint64) ([]model.Event, error) {
	var result []model.Event
	for _, event := range s.events[chamberID] {
		if event.Sequence > after {
			result = append(result, event)
		}
	}
	return result, nil
}

type fakeSender struct {
	failUntil int32 // accessed atomically, must be first for alignment
	mu        sync.Mutex
	sent      []Notification
}

func (s *fakeSender) Send(ctx context.Context, n Notification, _ Receiver) error {
	if atomic.LoadInt32(&s.failUntil) > 0 {
		return errors.New("simulated downstream outage")
	}
	s.mu.Lock()
	s.sent = append(s.sent, n)
	s.mu.Unlock()
	return nil
}

func (s *fakeSender) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

func newTestEvent(chamber string, sequence uint64, kind model.EventType, payload any) model.Event {
	event, err := model.NewEvent(chamber, kind, uuid.Nil, 0, payload, time.Date(2026, 9, 30, 10, 0, int(sequence), 0, time.UTC))
	if err != nil {
		panic(err)
	}
	event.Sequence = sequence
	return event
}

func newTestOutbox(t *testing.T, source EventSource, sender Sender, now func() time.Time) (*Outbox, *Roster, *MemoryOutbox) {
	t.Helper()
	configStore := NewMemoryConfigStore()
	outboxStore := NewMemoryOutbox()
	roster := NewRoster(configStore, now)
	outbox := NewOutbox(outboxStore, roster, source, sender, slog.New(slog.NewTextHandler(io.Discard, nil)), now)
	return outbox, roster, outboxStore
}

func criticalReceiver(id, chamber string) Receiver {
	return Receiver{
		ID: id, Name: id, Kind: ReceiverLog,
		Chambers: []string{chamber}, MinSeverity: SeverityCritical, Enabled: true,
	}
}

// Projecting the same events repeatedly must never create different notifications.
func TestProjectEventsIsIdempotentAcrossRescans(t *testing.T) {
	ctx := context.Background()
	now := fixedClock(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	source := &fakeSource{
		chambers: []string{"chamber-1"},
		events: map[string][]model.Event{
			"chamber-1": {
				newTestEvent("chamber-1", 1, model.EventEmergencyStarted, model.EmergencyRecord{Message: "stop"}),
			},
		},
	}
	sender := &fakeSender{}
	outbox, roster, _ := newTestOutbox(t, source, sender, now)
	if _, err := roster.Upsert(ctx, criticalReceiver("ops", "chamber-1")); err != nil {
		t.Fatal(err)
	}

	created, err := outbox.ProjectEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("first scan: created=%d want 1", created)
	}
	created, err = outbox.ProjectEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("repeat scan: created=%d want 0", created)
	}
	pending := outbox.Pending()
	if len(pending) != 1 {
		t.Fatalf("pending=%d want 1", len(pending))
	}
	derived := DeriveID(source.events["chamber-1"][0].ID, "ops")
	if pending[0].ID != derived {
		t.Fatalf("notification id is not derived from (event, receiver)")
	}
	if pending[0].SourceEventID != source.events["chamber-1"][0].ID {
		t.Fatalf("notification must carry source event id")
	}
}

// Restarting the process rebuilds the same outbox from the store; deterministic
// ids mean the restored notification is identical, not a new one.
func TestRestartDoesNotCreateDifferentNotifications(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	source := &fakeSource{
		chambers: []string{"chamber-1"},
		events: map[string][]model.Event{
			"chamber-1": {
				newTestEvent("chamber-1", 1, model.EventEmergencyStarted, model.EmergencyRecord{}),
			},
		},
	}
	sender := &fakeSender{}
	configStore := NewMemoryConfigStore()
	outboxStore := NewMemoryOutbox()
	roster := NewRoster(configStore, fixedClock(clock))
	if _, err := roster.Upsert(ctx, criticalReceiver("ops", "chamber-1")); err != nil {
		t.Fatal(err)
	}
	first := NewOutbox(outboxStore, roster, source, sender, slog.New(slog.NewTextHandler(io.Discard, nil)), fixedClock(clock))
	if _, err := first.ProjectEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.DispatchDue(ctx); err != nil {
		t.Fatal(err)
	}
	firstID := DeriveID(source.events["chamber-1"][0].ID, "ops")

	// Simulate restart: fresh in-memory projections, same persistent store.
	second := NewOutbox(outboxStore, roster, source, sender, slog.New(slog.NewTextHandler(io.Discard, nil)), fixedClock(clock))
	if err := second.Load(ctx); err != nil {
		t.Fatal(err)
	}
	created, err := second.ProjectEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("post-restart scan created=%d want 0", created)
	}
	items := second.Notifications("", "")
	if len(items) != 1 || items[0].ID != firstID {
		t.Fatalf("post-restart notification differs: %+v", items)
	}
	if items[0].Status != StatusDelivered {
		t.Fatalf("delivered status must survive restart, got %s", items[0].Status)
	}
}

// A failed send is retried with backoff and eventually succeeds; the notification
// is never duplicated, and every attempt is recorded.
func TestFailedDeliveryRetriesWithoutDuplicates(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	current := base
	now := func() time.Time { return current }
	source := &fakeSource{
		chambers: []string{"chamber-1"},
		events: map[string][]model.Event{
			"chamber-1": {
				newTestEvent("chamber-1", 1, model.EventEmergencyStarted, model.EmergencyRecord{}),
			},
		},
	}
	sender := &fakeSender{}
	atomic.StoreInt32(&sender.failUntil, 1)
	outbox, roster, _ := newTestOutbox(t, source, sender, now)
	if _, err := roster.Upsert(ctx, criticalReceiver("ops", "chamber-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ProjectEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := outbox.DispatchDue(ctx); err != nil {
		t.Fatal(err)
	}
	pending := outbox.Pending()
	if len(pending) != 1 || pending[0].Attempts != 1 || pending[0].LastError == "" {
		t.Fatalf("failed attempt not recorded: %+v", pending)
	}
	if !pending[0].NextAttemptAt.After(base) {
		t.Fatalf("backoff must schedule a future attempt")
	}
	if sender.sentCount() != 0 {
		t.Fatal("nothing should have been delivered while downstream is down")
	}

	// Before backoff elapses nothing is retried.
	current = base.Add(backoff(1) - time.Second)
	if err := outbox.DispatchDue(ctx); err != nil {
		t.Fatal(err)
	}
	if sender.sentCount() != 0 {
		t.Fatal("notification dispatched before backoff elapsed")
	}

	// After backoff, recovery succeeds.
	current = base.Add(backoff(1))
	atomic.StoreInt32(&sender.failUntil, 0)
	if err := outbox.DispatchDue(ctx); err != nil {
		t.Fatal(err)
	}
	if sender.sentCount() != 1 {
		t.Fatalf("deliveries=%d want 1", sender.sentCount())
	}
	if len(outbox.Pending()) != 0 {
		t.Fatal("pending list must be empty after delivery")
	}
	items := outbox.Notifications("", "")
	if len(items) != 1 || items[0].Status != StatusDelivered {
		t.Fatalf("notification must be delivered, got %+v", items)
	}
	records := outbox.Records(0)
	if len(records) != 2 {
		t.Fatalf("records=%d want 2 (one failure + one success)", len(records))
	}
	if records[0].Status != "delivered" || records[1].Status != "failed" {
		t.Fatalf("records newest-first want delivered,failed got %s,%s", records[0].Status, records[1].Status)
	}
}

// Exhausted retries move to failed and stay visible; manual retry requeues once.
func TestExhaustedRetriesMoveToFailedAndCanBeRetried(t *testing.T) {
	ctx := context.Background()
	current := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	now := func() time.Time { return current }
	source := &fakeSource{
		chambers: []string{"chamber-1"},
		events: map[string][]model.Event{
			"chamber-1": {
				newTestEvent("chamber-1", 1, model.EventEmergencyStarted, model.EmergencyRecord{}),
			},
		},
	}
	sender := &fakeSender{failUntil: 1}
	outbox, roster, _ := newTestOutbox(t, source, sender, now)
	if _, err := roster.Upsert(ctx, criticalReceiver("ops", "chamber-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ProjectEvents(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < defaultMaxAttempts; i++ {
		if err := outbox.DispatchDue(ctx); err != nil {
			t.Fatal(err)
		}
		current = current.Add(backoffCap + time.Minute)
	}
	items := outbox.Notifications("", "")
	if len(items) != 1 || items[0].Status != StatusFailed || items[0].Attempts != defaultMaxAttempts {
		t.Fatalf("notification should be failed after max attempts: %+v", items)
	}
	// Failed notifications are excluded from the automatic pending sweep.
	if err := outbox.DispatchDue(ctx); err != nil {
		t.Fatal(err)
	}
	if sender.sentCount() != 0 {
		t.Fatal("exhausted notification must not be auto-retried")
	}

	atomic.StoreInt32(&sender.failUntil, 0)
	if _, err := outbox.Retry(ctx, items[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := outbox.DispatchDue(ctx); err != nil {
		t.Fatal(err)
	}
	if sender.sentCount() != 1 {
		t.Fatalf("manual retry should deliver once, got %d", sender.sentCount())
	}
}

// Routing selects receivers by chamber and minimum severity.
func TestRoutingByChamberAndSeverity(t *testing.T) {
	ctx := context.Background()
	now := fixedClock(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))
	events := []model.Event{
		newTestEvent("chamber-1", 1, model.EventEmergencyStarted, model.EmergencyRecord{}),
		newTestEvent("chamber-2", 2, model.EventPlanCancelled, map[string]any{"note": "x"}),
	}
	source := &fakeSource{chambers: []string{"chamber-1", "chamber-2"}, events: map[string][]model.Event{
		"chamber-1": {events[0]}, "chamber-2": {events[1]},
	}}
	sender := &fakeSender{}
	outbox, roster, _ := newTestOutbox(t, source, sender, now)
	if _, err := roster.Upsert(ctx, Receiver{ID: "c1-critical", Name: "c1", Kind: ReceiverLog, Chambers: []string{"chamber-1"}, MinSeverity: SeverityCritical, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := roster.Upsert(ctx, Receiver{ID: "all-warning", Name: "all", Kind: ReceiverLog, MinSeverity: SeverityWarning, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := roster.Upsert(ctx, Receiver{ID: "c2-notice-off", Name: "off", Kind: ReceiverLog, Chambers: []string{"chamber-2"}, MinSeverity: SeverityNotice, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.ProjectEvents(ctx); err != nil {
		t.Fatal(err)
	}
	items := outbox.Notifications("", "")
	counts := map[string]int{}
	for _, item := range items {
		counts[item.ReceiverID]++
	}
	// chamber-1 emergency(critical) -> c1-critical and all-warning
	// chamber-2 cancellation(warning) -> all-warning only (disabled receiver skipped)
	if counts["c1-critical"] != 1 || counts["all-warning"] != 2 || counts["c2-notice-off"] != 0 {
		t.Fatalf("unexpected routing: %+v", counts)
	}
}

// All tracked event kinds materialize with the expected severity.
func TestAllSafetyEventTypesClassify(t *testing.T) {
	cases := []struct {
		kind     model.EventType
		severity Severity
	}{
		{model.EventEmergencyStarted, SeverityCritical},
		{model.EventEmergencySettled, SeverityWarning},
		{model.EventPlanCancelled, SeverityWarning},
		{model.EventReplayWarning, SeverityWarning},
	}
	for _, tc := range cases {
		meaning, ok := classifyEvent(newTestEvent("chamber-1", 1, tc.kind, map[string]any{}))
		if !ok {
			t.Fatalf("%s not classified", tc.kind)
		}
		if meaning.severity != tc.severity {
			t.Fatalf("%s severity=%s want %s", tc.kind, meaning.severity, tc.severity)
		}
	}
	if _, ok := classifyEvent(newTestEvent("chamber-1", 1, model.EventCycleCreated, nil)); ok {
		t.Fatal("non-safety events must not produce notifications")
	}
}

func TestWebhookSenderIncludesSourceEventHeaders(t *testing.T) {
	var seen struct {
		sync.Mutex
		header http.Header
		body   map[string]any
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Lock()
		defer seen.Unlock()
		seen.header = r.Header.Clone()
		body := map[string]any{}
		_ = decodeJSONBody(r, &body)
		seen.body = body
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sourceEventID := uuid.New()
	notification := Notification{
		ID: uuid.New(), SourceEventID: sourceEventID, ChamberID: "chamber-1",
		EventType: model.EventEmergencyStarted, Severity: SeverityCritical,
		Title: "Emergency stop requested", Summary: "boom",
	}
	receiver := Receiver{ID: "wh", Name: "wh", Kind: ReceiverWebhook, Target: server.URL, MinSeverity: SeverityNotice, Enabled: true}
	if err := NewWebhookSender(server.Client()).Send(context.Background(), notification, receiver); err != nil {
		t.Fatal(err)
	}
	seen.Lock()
	defer seen.Unlock()
	if seen.header.Get("X-Lockflow-Event") != sourceEventID.String() {
		t.Fatalf("source event header missing: %v", seen.header)
	}
	if seen.body["source_event_id"] != sourceEventID.String() {
		t.Fatalf("body missing source_event_id: %v", seen.body)
	}
}

func TestWebhookSenderNon2xxIsRetryableError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream down"))
	}))
	defer server.Close()
	sender := NewWebhookSender(server.Client())
	err := sender.Send(context.Background(), Notification{ID: uuid.New()}, Receiver{Kind: ReceiverWebhook, Target: server.URL})
	if err == nil {
		t.Fatal("502 must surface as an error so the outbox retries")
	}
}
