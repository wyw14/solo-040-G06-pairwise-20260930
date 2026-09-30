package notify

import (
	"context"

	"github.com/wyw14/cry-96/internal/model"
)

// ObserveEvent satisfies journal.EventObserver. Historical events must not be
// replayed through this hook: only freshly committed events arrive here, and
// Ingest itself is idempotent regardless.
func (o *Outbox) ObserveEvent(ctx context.Context, event model.Event) error {
	return o.Ingest(ctx, event)
}
