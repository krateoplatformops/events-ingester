package router

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/krateoplatformops/events-ingester/internal/k8sevents"
	"github.com/krateoplatformops/events-ingester/internal/telemetry"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// EventHandler handles final processed events
type EventHandler interface {
	Handle(e corev1.Event)
}

// EventRouter routes Kubernetes Events to a handler with throttling,
// deduplication and multi-namespace support.
type EventRouter struct {
	handler        EventHandler
	informers      []cache.SharedInformer
	throttlePeriod time.Duration
	log            *slog.Logger
	metrics        *telemetry.Metrics
	partitions     PartitionRangeProvider
}

type EventRouterOpts struct {
	RESTClient     rest.Interface
	Log            *slog.Logger
	Handler        EventHandler
	ResyncInterval time.Duration
	ThrottlePeriod time.Duration

	// Multiple namespaces or nil -> watch everything
	Namespaces []string
	Metrics    *telemetry.Metrics
	Partitions PartitionRangeProvider
}

func NewEventRouter(opts EventRouterOpts) *EventRouter {
	namespaces := opts.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{corev1.NamespaceAll}
	}

	var informers []cache.SharedInformer
	for _, ns := range namespaces {
		lw := cache.NewListWatchFromClient(
			opts.RESTClient,
			"events",
			ns,
			fields.Everything(),
		)

		si := cache.NewSharedInformer(lw, &corev1.Event{}, opts.ResyncInterval)
		informers = append(informers, si)
	}

	return &EventRouter{
		informers:      informers,
		handler:        opts.Handler,
		throttlePeriod: opts.ThrottlePeriod,
		log:            opts.Log,
		metrics:        opts.Metrics,
		partitions:     opts.Partitions,
	}
}

func (er *EventRouter) Run(stop <-chan struct{}) {
	defer utilruntime.HandleCrash()

	for _, inf := range er.informers {
		inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    er.OnAdd,
			UpdateFunc: er.OnUpdate,
			DeleteFunc: er.OnDelete,
		})

		go inf.Run(stop)
	}

	// Wait for all informers to sync
	for _, inf := range er.informers {
		if !cache.WaitForCacheSync(stop, inf.HasSynced) {
			err := fmt.Errorf("timed out waiting for caches to sync")
			utilruntime.HandleError(err)
			er.log.Error("cache sync failed", slog.Any("err", err))
			return
		}
	}

	er.log.Info("EventRouter started")
	<-stop
	er.log.Info("EventRouter stopped")
}

func (er *EventRouter) OnAdd(obj interface{}) {
	event := obj.(*corev1.Event)
	er.onEvent(event)
}

// Dedup by ResourceVersion to prevent noisy updates
func (er *EventRouter) OnUpdate(oldObj, newObj interface{}) {
	oldEvent, ok1 := oldObj.(*corev1.Event)
	newEvent, ok2 := newObj.(*corev1.Event)
	if !ok1 || !ok2 {
		return
	}

	if oldEvent.ResourceVersion == newEvent.ResourceVersion {
		// No real change — skip
		er.metrics.IncEventsDropped(context.Background(), "same_resource_version")
		return
	}

	er.onEvent(newEvent)
}

// Tombstone-safe delete
func (er *EventRouter) OnDelete(obj any) {
	// events deleted are irrelevant to storage; ignore
}

func (er *EventRouter) onEvent(event *corev1.Event) {
	ctx := context.Background()
	er.metrics.IncEventsReceived(ctx)

	ts := k8sevents.Timestamp(*event)

	if er.throttlePeriod > 0 && time.Since(ts) > er.throttlePeriod {
		er.metrics.IncEventsDropped(ctx, "throttled")
		return
	}

	if er.partitions != nil {
		rng, ok := er.partitions.Current(ctx)
		if ok && !rng.Contains(ts) {
			er.metrics.IncEventsDropped(ctx, "outside_partition_range")
			er.log.Warn("skipping event outside k8s_events partition range",
				slog.String("eventTimestamp", ts.Format(time.RFC3339Nano)),
				slog.String("minPartitionStart", rng.MinPartitionStart.Format(time.RFC3339Nano)),
				slog.String("maxPartitionEnd", rng.MaxPartitionEnd.Format(time.RFC3339Nano)),
				slog.String("uid", string(event.UID)),
				slog.String("resourceVersion", event.ResourceVersion),
				slog.String("namespace", event.Namespace),
				slog.String("eventName", event.Name),
				slog.String("objectName", event.InvolvedObject.Name),
			)
			return
		}
	}

	er.log.Debug("K8s event received",
		slog.String("namespace", event.Namespace),
		slog.String("reason", event.Reason),
		slog.String("message", event.Message),
		slog.String("object", event.InvolvedObject.Name),
		slog.String("rv", event.ResourceVersion),
	)

	// Skip events already containing composition ID
	if hasCompositionId(event) {
		er.log.Debug("Skipping event with existing Composition ID",
			slog.String("namespace", event.Namespace),
			slog.String("object", event.InvolvedObject.Name),
		)
		er.metrics.IncEventsDropped(ctx, "already_labeled")
		return
	}

	// Dispatch
	er.handler.Handle(*event.DeepCopy())
	er.metrics.IncEventsDispatched(ctx)
}
