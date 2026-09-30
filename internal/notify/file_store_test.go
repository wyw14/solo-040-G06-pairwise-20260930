package notify

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/model"
)

// Durable file storage must restore notifications and delivery records across
// process restarts without duplicating or inventing notifications.
func TestFileStoreSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := fixedClock(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC))

	first, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	roster := NewRoster(first, now)
	if _, err := roster.Upsert(ctx, criticalReceiver("ops", "chamber-1")); err != nil {
		t.Fatal(err)
	}
	source := &fakeSource{
		chambers: []string{"chamber-1"},
		events: map[string][]model.Event{
			"chamber-1": {
				newTestEvent("chamber-1", 1, model.EventEmergencyStarted, model.EmergencyRecord{}),
			},
		},
	}
	eventID := source.events["chamber-1"][0].ID
	outbox := NewOutbox(first, roster, source, NewLogSender(nil), nil, now)
	if _, err := outbox.ProjectEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := outbox.DispatchDue(ctx); err != nil {
		t.Fatal(err)
	}

	// Restart: brand new store objects pointed at the same directory.
	second, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	restoredRoster := NewRoster(second, now)
	if err := restoredRoster.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if len(restoredRoster.List()) != 1 {
		t.Fatalf("receiver config did not survive restart")
	}
	restored := NewOutbox(second, restoredRoster, source, NewLogSender(nil), nil, now)
	if err := restored.Load(ctx); err != nil {
		t.Fatal(err)
	}
	created, err := restored.ProjectEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if created != 0 {
		t.Fatalf("restart rescan created=%d want 0 (deterministic dedupe)", created)
	}
	items := restored.Notifications("", "")
	if len(items) != 1 {
		t.Fatalf("notifications restored=%d want 1", len(items))
	}
	if items[0].ID != DeriveID(eventID, "ops") {
		t.Fatal("restored notification has a different id")
	}
	if items[0].Status != StatusDelivered {
		t.Fatalf("delivered status lost across restart: %s", items[0].Status)
	}
	records := restored.Records(0)
	if len(records) != 1 || records[0].Status != "delivered" {
		t.Fatalf("delivery records lost across restart: %+v", records)
	}
}
