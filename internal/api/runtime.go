package api

import (
	"context"
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/interlock"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/lease"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/monitor"
	"github.com/wyw14/cry-96/internal/notify"
	"github.com/wyw14/cry-96/internal/safety"
	"github.com/wyw14/cry-96/internal/schedule"
	"github.com/wyw14/cry-96/internal/signal"
)

// Runtime contains the long-lived component graph used by HTTP handlers.
type Runtime struct {
	Store           journal.Store
	Recorder        *journal.Recorder
	GateState       *gate.State
	GateCommands    *gate.Commands
	Gates           *gate.Controller
	SignalStore     *signal.Store
	SignalPublisher *signal.Publisher
	Signals         *signal.Service
	AdmissionQueue  *admission.Queue
	Admission       *admission.Service
	Planner         *schedule.Planner
	Schedule        *schedule.Service
	Hydraulics      *hydraulics.Controller
	Interlocks      *interlock.Coordinator
	Cycles          *cycle.Coordinator
	CycleService    *cycle.Service
	Leases          *lease.Service
	LeaseSweeper    *lease.Sweeper
	Safety          *safety.Service
	Snapshot        *monitor.Snapshot
	Incidents       *monitor.IncidentReview
	Health          *monitor.HealthRegistry
	Recovery        *cycle.Recovery
	NotifyRoster    *notify.Roster
	NotifyOutbox    *notify.Outbox
	Chambers        []string
	Now             func() time.Time
}

func NewRuntime(ctx context.Context, dataDirectory string) (*Runtime, error) {
	now := time.Now
	store, err := journal.NewRuntimeStore(dataDirectory)
	if err != nil {
		return nil, err
	}
	recorder := journal.NewRecorder(store, now)
	chambers := []string{"chamber-1", "chamber-2"}
	gateState := gate.NewState(chambers...)
	gateCommands := gate.NewCommands()
	gateController := gate.NewController(gateState, gateCommands, recorder, now)
	signalStore := signal.NewStore()
	signalPublisher := signal.NewPublisher(signalStore, recorder, now)
	signalService := signal.NewService(signalStore)
	admissionQueue := admission.NewQueue()
	admissionController := admission.NewController(admissionQueue, gateState, signalStore, now)
	admissionService := admission.NewService(admissionController, recorder, now)
	leaseRegistry := lease.NewRegistry()
	leaseService := lease.NewService(leaseRegistry, admissionController, recorder, now)
	leaseSweeper := lease.NewSweeper(leaseRegistry, now)
	planner := schedule.NewPlanner(admissionService, recorder, now)
	scheduleService := schedule.NewService(planner, signalPublisher)
	ramp := hydraulics.NewRamp(25*time.Millisecond, now)
	hydraulicsController := hydraulics.NewController(gateCommands, recorder, ramp, now)
	barrier := safety.NewBarrier()
	var safetyCoordinator *safety.Coordinator
	levelByChamber := func(chamberID string) float64 {
		samples := hydraulicsController.WindowSamples(chamberID)
		if len(samples) == 0 {
			return 0
		}
		return samples[len(samples)-1].Meters
	}
	emergencyActive := func(chamberID string) bool {
		return safetyCoordinator != nil && safetyCoordinator.Active(chamberID)
	}
	interlockCoordinator := interlock.NewCoordinator(
		interlock.DefaultPolicy(), interlock.NewArbiter(), gateController,
		levelByChamber, emergencyActive,
	)
	cycleCoordinator := cycle.NewCoordinator(gateController, hydraulicsController, recorder, now)
	cycleService := cycle.NewService(cycleCoordinator, admissionService)
	safetyCoordinator = safety.NewCoordinator(gateCommands, barrier, recorder, now)
	safetyService := safety.NewService(safetyCoordinator, cycleCoordinator)
	signalService.BindTransitLookup(cycleCoordinator)
	replayer := journal.NewReplayer(store)
	recovery := cycle.NewRecovery(replayer, cycleCoordinator, recorder)
	if _, err := recovery.RestoreAll(ctx); err != nil {
		return nil, err
	}
	health := monitor.NewHealthRegistry()
	for _, chamberID := range chambers {
		for _, device := range []model.DeviceKind{
			model.DeviceUpstreamGate, model.DeviceDownstreamGate,
			model.DeviceFillValve, model.DeviceDrainValve,
		} {
			health.Observe(chamberID, device, true, "telemetry current", now())
		}
	}
	configStore, outboxStore, err := notify.NewRuntimeStores(dataDirectory)
	if err != nil {
		return nil, err
	}
	notifyRoster := notify.NewRoster(configStore, now)
	if err := notifyRoster.Load(ctx); err != nil {
		return nil, err
	}
	if len(notifyRoster.List()) == 0 {
		// Seed a built-in local duty desk so safety events are observable before
		// external webhooks are configured. Operators can disable or replace it.
		if _, err := notifyRoster.Upsert(ctx, notify.Receiver{
			ID: "duty-desk", Name: "Local duty desk", Kind: notify.ReceiverLog,
			MinSeverity: notify.SeverityNotice, Enabled: true,
		}); err != nil {
			return nil, err
		}
	}
	sender := notify.NewRoutingSender(nil, nil)
	notifyOutbox := notify.NewOutbox(outboxStore, notifyRoster, store, sender, nil, now)
	if err := notifyOutbox.Load(ctx); err != nil {
		return nil, err
	}
	return &Runtime{
		Store: store, Recorder: recorder, GateState: gateState,
		GateCommands: gateCommands, Gates: gateController,
		SignalStore: signalStore, SignalPublisher: signalPublisher, Signals: signalService,
		AdmissionQueue: admissionController.Queue(), Admission: admissionService,
		Planner: planner, Schedule: scheduleService, Hydraulics: hydraulicsController,
		Interlocks: interlockCoordinator, Cycles: cycleCoordinator, CycleService: cycleService,
		Leases: leaseService, LeaseSweeper: leaseSweeper, Safety: safetyService,
		Snapshot:  monitor.NewSnapshot(cycleCoordinator, gateState, admissionService, signalService, safetyService, now),
		Incidents: monitor.NewIncidentReview(store), Health: health, Recovery: recovery,
		NotifyRoster: notifyRoster, NotifyOutbox: notifyOutbox,
		Chambers: chambers, Now: now,
	}, nil
}
