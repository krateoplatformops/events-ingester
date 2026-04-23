package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/krateoplatformops/events-ingester/internal/batch"
	"github.com/krateoplatformops/events-ingester/internal/k8sevents"
	"github.com/krateoplatformops/events-ingester/internal/objects"
	"github.com/krateoplatformops/events-ingester/internal/queue"
	"github.com/krateoplatformops/events-ingester/internal/telemetry"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

type IngesterOpts struct {
	RESTConfig  *rest.Config
	Queue       queue.Queuer
	Log         *slog.Logger
	RecordChan  chan<- batch.InsertRecord // NEW
	ClusterName string                    // opzionale ma utile
	Metrics     *telemetry.Metrics
}

func NewIngester(opts IngesterOpts) (EventHandler, error) {
	objectResolver, err := objects.NewObjectResolver(opts.RESTConfig)
	if err != nil {
		return nil, err
	}

	return &ingester{
		objectResolver: objectResolver,
		notifyQueue:    opts.Queue,
		log:            opts.Log,
		recordChan:     opts.RecordChan,
		clusterName:    opts.ClusterName,
		metrics:        opts.Metrics,
	}, nil
}

var _ EventHandler = (*ingester)(nil)

type ingester struct {
	objectResolver *objects.ObjectResolver
	notifyQueue    queue.Queuer
	log            *slog.Logger
	recordChan     chan<- batch.InsertRecord
	clusterName    string
	metrics        *telemetry.Metrics
}

func (ing *ingester) Handle(evt corev1.Event) {
	ctx := context.Background()
	ref := &evt.InvolvedObject

	lookupStarted := time.Now()
	lookupResult := "found"
	compositionId, err := findCompositionID(ing.objectResolver, ref, ing.log)
	if err != nil {
		if !errors.Is(err, ErrCompositionIdNotFound) {
			lookupResult = "error"
			ing.metrics.RecordCompositionLookupDuration(ctx, time.Since(lookupStarted), lookupResult)
			ing.log.Error("unable to look for composition id",
				slog.String("involvedObject", ref.Name),
				slog.Any("err", err),
			)
			ing.metrics.IncEventsDropped(ctx, "composition_lookup_error")
			return
		}
		lookupResult = "not_found"
	} else if compositionId == "" {
		lookupResult = "not_found"
	}
	ing.metrics.RecordCompositionLookupDuration(ctx, time.Since(lookupStarted), lookupResult)

	ing.log.Debug(evt.Message,
		slog.String("name", evt.Name),
		slog.String("kind", ref.Kind),
		slog.String("reason", evt.Reason),
		slog.String("compositionId", compositionId),
	)

	rec := ing.buildRecord(evt, compositionId)
	if rec.UID == "" {
		ing.metrics.IncRecordBuildFailure(ctx, "missing_uid")
		ing.metrics.IncEventsDropped(ctx, "invalid_record")
		return
	}

	job := &batch.InsertRecordJob{
		Record: rec,
		Input:  ing.recordChan,
	}

	ing.notifyQueue.Push(job)
}

func (ing *ingester) buildRecord(evt corev1.Event, compositionID string) batch.InsertRecord {
	created := k8sevents.Timestamp(evt)

	// Minify and label
	out := minifyEvent(evt)

	labels := out.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[keyCompositionID] = compositionID
	out.SetLabels(labels)

	raw, err := json.Marshal(out)
	if err != nil {
		ing.log.Error("failed to encode event as JSON", slog.Any("err", err))
		ing.metrics.IncRecordBuildFailure(context.Background(), "marshal_error")
		return batch.InsertRecord{}
	}

	api := evt.InvolvedObject.APIVersion
	kind := evt.InvolvedObject.Kind
	resourceKind := kind
	if api != "" {
		resourceKind = api + "." + kind
	}

	cid := pgtype.UUID{Valid: false}
	if compositionID != "" {
		uid, err := uuid.Parse(compositionID)
		if err == nil {
			cid = pgtype.UUID{
				Bytes: uid,
				Valid: true,
			}
		}
	}

	involvedObjectUID := pgtype.Text{Valid: false}
	if uid := string(evt.InvolvedObject.UID); uid != "" {
		involvedObjectUID = pgtype.Text{
			String: uid,
			Valid:  true,
		}
	}

	return batch.InsertRecord{
		CreatedAt:         created,
		ClusterName:       ing.clusterName,
		UID:               string(evt.UID),
		GlobalUID:         fmt.Sprintf("%s:%s", ing.clusterName, evt.UID),
		Namespace:         evt.Namespace,
		ResourceKind:      resourceKind,
		ResourceName:      evt.InvolvedObject.Name,
		InvolvedObjectUID: involvedObjectUID,
		EventType:         evt.Type,
		Reason:            evt.Reason,
		Message:           evt.Message,
		CompositionID:     cid,
		ResourceVersion:   evt.ResourceVersion,
		Raw:               raw,
	}
}
