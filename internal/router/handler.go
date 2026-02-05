package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krateoplatformops/events-ingester/internal/batch"
	"github.com/krateoplatformops/events-ingester/internal/objects"
	"github.com/krateoplatformops/events-ingester/internal/queue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

type IngesterOpts struct {
	RESTConfig  *rest.Config
	Queue       queue.Queuer
	Pool        *pgxpool.Pool
	Log         *slog.Logger
	RecordChan  chan<- batch.InsertRecord // NEW
	ClusterName string                    // opzionale ma utile
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
	}, nil
}

var _ EventHandler = (*ingester)(nil)

type ingester struct {
	objectResolver *objects.ObjectResolver
	notifyQueue    queue.Queuer
	pool           *pgxpool.Pool
	log            *slog.Logger
	recordChan     chan<- batch.InsertRecord
	clusterName    string
}

func (ing *ingester) Handle(evt corev1.Event) {
	ref := &evt.InvolvedObject

	compositionId, err := findCompositionID(ing.objectResolver, ref, ing.log)
	if err != nil {
		if !errors.Is(err, ErrCompositionIdNotFound) {
			ing.log.Error("unable to look for composition id",
				slog.String("involvedObject", ref.Name),
				slog.Any("err", err),
			)
			return
		}
	}

	ing.log.Debug(evt.Message,
		slog.String("name", evt.Name),
		slog.String("kind", ref.Kind),
		slog.String("reason", evt.Reason),
		slog.String("compositionId", compositionId),
	)

	rec := ing.buildRecord(evt, compositionId)
	if rec.UID == "" {
		return
	}

	job := &batch.InsertRecordJob{
		Record: rec,
		Input:  ing.recordChan,
	}

	ing.notifyQueue.Push(job)
}

func (ing *ingester) buildRecord(evt corev1.Event, compositionID string) batch.InsertRecord {
	// Choose best timestamp
	created := evt.EventTime.Time
	if created.IsZero() {
		created = evt.CreationTimestamp.Time
	}

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

	return batch.InsertRecord{
		CreatedAt:       created,
		ClusterName:     ing.clusterName,
		UID:             string(evt.UID),
		GlobalUID:       fmt.Sprintf("%s:%s", ing.clusterName, evt.UID),
		Namespace:       evt.Namespace,
		ResourceKind:    resourceKind,
		ResourceName:    evt.InvolvedObject.Name,
		EventType:       evt.Type,
		Reason:          evt.Reason,
		Message:         evt.Message,
		CompositionID:   cid,
		ResourceVersion: evt.ResourceVersion,
		Raw:             raw,
	}
}
