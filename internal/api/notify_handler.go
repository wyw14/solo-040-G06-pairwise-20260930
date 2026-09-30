package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/notify"
)

func pendingNotificationsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, runtime.NotifyOutbox.Pending())
	}
}

func notificationsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := notify.NotificationStatus(r.URL.Query().Get("status"))
		if status != "" && status != notify.StatusPending &&
			status != notify.StatusDelivered && status != notify.StatusFailed {
			writeError(w, http.StatusBadRequest, errors.New("status must be pending, delivered or failed"))
			return
		}
		writeJSON(w, http.StatusOK, runtime.NotifyOutbox.Notifications(r.URL.Query().Get("chamber_id"), status))
	}
}

func deliveryRecordsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if value := r.URL.Query().Get("limit"); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 0 {
				writeError(w, http.StatusBadRequest, errors.New("limit must be a non-negative integer"))
				return
			}
			limit = parsed
		}
		writeJSON(w, http.StatusOK, runtime.NotifyOutbox.Records(limit))
	}
}

func retryNotificationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "notificationID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		notification, err := runtime.NotifyOutbox.Retry(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		// Retry only requeues; attempt delivery immediately so the operator sees
		// the result. A failed send still leaves the notification pending/failed
		// and never affects the originating safety action.
		_ = runtime.NotifyOutbox.DispatchDue(r.Context())
		notification, _ = runtime.NotifyOutbox.Get(id)
		writeJSON(w, http.StatusAccepted, notification)
	}
}

type receiverInput struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Kind        string   `json:"kind"`
	Target      string   `json:"target"`
	Chambers    []string `json:"chambers"`
	MinSeverity string   `json:"min_severity"`
	Enabled     *bool    `json:"enabled"`
}

func listReceiversHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, runtime.NotifyRoster.List())
	}
}

func upsertReceiverHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input receiverInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		receiver := notify.Receiver{
			ID: input.ID, Name: input.Name, Kind: notify.ReceiverKind(input.Kind),
			Target: input.Target, Chambers: input.Chambers,
			MinSeverity: notify.Severity(input.MinSeverity),
			Enabled:     true,
		}
		if input.Enabled != nil {
			receiver.Enabled = *input.Enabled
		}
		saved, err := runtime.NotifyRoster.Upsert(r.Context(), receiver)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		// Route the new/changed roster against history right away.
		if _, err := runtime.NotifyOutbox.ProjectEvents(r.Context()); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, saved)
	}
}

func setReceiverEnabledHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Enabled bool `json:"enabled"`
		}
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		receiver, err := runtime.NotifyRoster.SetEnabled(r.Context(), chi.URLParam(r, "receiverID"), input.Enabled)
		if err != nil {
			writeError(w, http.StatusNotFound, err)
			return
		}
		if _, err := runtime.NotifyOutbox.ProjectEvents(r.Context()); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, receiver)
	}
}
