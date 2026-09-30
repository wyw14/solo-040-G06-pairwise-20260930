package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-96/internal/notify"
)

// pendingNotificationsHandler lists envelopes that still need delivery work,
// including failed ones waiting on backoff and dead-lettered ones.
func pendingNotificationsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pending, err := runtime.Notifications.Pending(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"pending": pending, "count": len(pending),
		})
	}
}

// deliveriesHandler returns the full delivery ledger with attempt history.
func deliveriesHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		records, err := runtime.Notifications.Deliveries(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"deliveries": records, "count": len(records),
		})
	}
}

func notificationRoutesHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"routes": runtime.NotificationRoutes.List(),
		})
	}
}

type notifyRouteInput struct {
	Name     string          `json:"name"`
	Chambers []string        `json:"chambers"`
	MinLevel notify.Severity `json:"min_level"`
	Channel  notify.Channel  `json:"channel"`
	Target   string          `json:"target"`
	Enabled  *bool           `json:"enabled"`
}

// upsertNotificationRouteHandler lets duty staff configure on-call sinks at
// runtime. Existing envelopes keep their channel snapshot, so routing changes
// never break in-flight notifications.
func upsertNotificationRouteHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input notifyRouteInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		enabled := true
		if existing, ok := runtime.NotificationRoutes.Get(input.Name); ok {
			enabled = existing.Enabled
		}
		if input.Enabled != nil {
			enabled = *input.Enabled
		}
		route := notify.Route{
			Name: input.Name, Chambers: input.Chambers,
			MinLevel: input.MinLevel, Channel: input.Channel,
			Target: input.Target, Enabled: enabled,
		}
		if err := runtime.NotificationRoutes.Upsert(route); err != nil {
			if errors.Is(err, notify.ErrInvalidRoute) {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, route)
	}
}

// retryNotificationHandler reopens a failed/dead envelope immediately with a
// fresh attempt budget.
func retryNotificationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		envelope, err := runtime.Notifications.Retry(r.Context(), chi.URLParam(r, "notificationID"))
		if err != nil {
			if errors.Is(err, notify.ErrEnvelopeNotFound) {
				writeError(w, http.StatusNotFound, err)
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusAccepted, envelope)
	}
}
