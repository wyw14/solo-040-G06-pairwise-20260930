package notify

import (
	"errors"
	"sort"
	"sync"
)

// RouteRegistry holds the configured on-call sinks. It is safe for concurrent
// use and may be reloaded at runtime without touching pending envelopes.
type RouteRegistry struct {
	mu     sync.RWMutex
	routes map[string]Route
}

func NewRouteRegistry() *RouteRegistry {
	return &RouteRegistry{routes: make(map[string]Route)}
}

var (
	ErrRouteNotFound = errors.New("notification route not found")
	ErrRouteExists   = errors.New("notification route already exists")
	ErrInvalidRoute  = errors.New("notification route is invalid")
)

// Upsert creates or replaces a route by name.
func (r *RouteRegistry) Upsert(route Route) error {
	if err := validateRoute(route); err != nil {
		return err
	}
	r.mu.Lock()
	r.routes[route.Name] = route
	r.mu.Unlock()
	return nil
}

func validateRoute(route Route) error {
	if route.Name == "" {
		return errors.Join(ErrInvalidRoute, errors.New("route name is required"))
	}
	switch route.Channel {
	case ChannelLog, ChannelWebhook, ChannelNoop:
	default:
		return errors.Join(ErrInvalidRoute, errors.New("unsupported channel "+string(route.Channel)))
	}
	if route.Channel == ChannelWebhook && route.Target == "" {
		return errors.Join(ErrInvalidRoute, errors.New("webhook route requires a target"))
	}
	if severityRank(route.MinLevel) == 0 {
		return errors.Join(ErrInvalidRoute, errors.New("route requires a valid min_level"))
	}
	return nil
}

// Remove deletes a route. Existing envelopes keep their resolved channel and
// target snapshot so in-flight notifications remain deliverable.
func (r *RouteRegistry) Remove(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[name]; !ok {
		return ErrRouteNotFound
	}
	delete(r.routes, name)
	return nil
}

func (r *RouteRegistry) Get(name string) (Route, bool) {
	r.mu.RLock()
	route, ok := r.routes[name]
	r.mu.RUnlock()
	return route, ok
}

// List returns routes sorted by name for stable views.
func (r *RouteRegistry) List() []Route {
	r.mu.RLock()
	result := make([]Route, 0, len(r.routes))
	for _, route := range r.routes {
		copied := route
		copied.Chambers = append([]string(nil), route.Chambers...)
		result = append(result, copied)
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// targets resolves every enabled route that should receive an event.
func (r *RouteRegistry) targets(chamberID string, severity Severity) []Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var matched []Route
	for _, route := range r.routes {
		if route.matches(chamberID, severity) {
			matched = append(matched, route)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })
	return matched
}
