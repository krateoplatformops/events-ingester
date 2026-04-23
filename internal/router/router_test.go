package router

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/krateoplatformops/events-ingester/internal/k8sevents"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type staticPartitionRangeProvider struct {
	rng PartitionRange
	ok  bool
}

func (p staticPartitionRangeProvider) Current(_ context.Context) (PartitionRange, bool) {
	return p.rng, p.ok
}

type fakeHandler struct {
	count     int
	lastEvent corev1.Event
}

func (h *fakeHandler) Handle(e corev1.Event) {
	h.count++
	h.lastEvent = e
}

func TestRouterSkipsEventBeforeMinPartitionStart(t *testing.T) {
	handler := &fakeHandler{}
	router := &EventRouter{
		handler: handler,
		log:     testLogger(),
		partitions: staticPartitionRangeProvider{ok: true, rng: PartitionRange{
			MinPartitionStart: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
			MaxPartitionEnd:   time.Date(2026, 4, 24, 10, 0, 0, 0, time.UTC),
		}},
	}

	router.onEvent(newEventAt(time.Date(2026, 4, 23, 9, 59, 59, 0, time.UTC)))

	if handler.count != 0 {
		t.Fatalf("expected handler not to be called, got %d calls", handler.count)
	}
}

func TestRouterSkipsEventAtOrAfterMaxPartitionEnd(t *testing.T) {
	handler := &fakeHandler{}
	router := &EventRouter{
		handler: handler,
		log:     testLogger(),
		partitions: staticPartitionRangeProvider{ok: true, rng: PartitionRange{
			MinPartitionStart: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
			MaxPartitionEnd:   time.Date(2026, 4, 24, 10, 0, 0, 0, time.UTC),
		}},
	}

	router.onEvent(newEventAt(time.Date(2026, 4, 24, 10, 0, 0, 0, time.UTC)))

	if handler.count != 0 {
		t.Fatalf("expected handler not to be called, got %d calls", handler.count)
	}
}

func TestRouterPassesThroughEventWithinPartitionRange(t *testing.T) {
	handler := &fakeHandler{}
	router := &EventRouter{
		handler: handler,
		log:     testLogger(),
		partitions: staticPartitionRangeProvider{ok: true, rng: PartitionRange{
			MinPartitionStart: time.Date(2026, 4, 23, 10, 0, 0, 0, time.UTC),
			MaxPartitionEnd:   time.Date(2026, 4, 24, 10, 0, 0, 0, time.UTC),
		}},
	}

	evt := newEventAt(time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC))
	router.onEvent(evt)

	if handler.count != 1 {
		t.Fatalf("expected handler to be called once, got %d calls", handler.count)
	}
	if !k8sevents.Timestamp(handler.lastEvent).Equal(k8sevents.Timestamp(*evt)) {
		t.Fatalf("expected dispatched event timestamp to match original")
	}
}

func TestBuildRecordUsesCanonicalEventTimestamp(t *testing.T) {
	ing := &ingester{
		log:         testLogger(),
		clusterName: "test-cluster",
	}

	last := time.Date(2026, 4, 23, 8, 0, 0, 0, time.FixedZone("CEST", 2*60*60))
	eventTime := time.Date(2026, 4, 23, 10, 0, 0, 0, time.FixedZone("EEST", 3*60*60))
	evt := corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "event-name",
			Namespace:       "default",
			UID:             types.UID("evt-uid"),
			ResourceVersion: "42",
		},
		EventTime:     metav1.MicroTime{Time: eventTime},
		LastTimestamp: metav1.Time{Time: last},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod",
			Name: "demo-pod",
			UID:  types.UID("obj-uid"),
		},
	}

	record := ing.buildRecord(evt, "")
	want := k8sevents.Timestamp(evt)
	if !record.CreatedAt.Equal(want) {
		t.Fatalf("expected created_at %s, got %s", want, record.CreatedAt)
	}

	evt.EventTime = metav1.MicroTime{}
	record = ing.buildRecord(evt, "")
	want = k8sevents.Timestamp(evt)
	if !record.CreatedAt.Equal(want) {
		t.Fatalf("expected fallback created_at %s, got %s", want, record.CreatedAt)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newEventAt(ts time.Time) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "event-name",
			Namespace:       "default",
			UID:             types.UID("evt-uid"),
			ResourceVersion: "123",
		},
		EventTime:     metav1.MicroTime{Time: ts},
		LastTimestamp: metav1.Time{Time: ts.Add(-time.Minute)},
		InvolvedObject: corev1.ObjectReference{
			Name: "demo-object",
		},
	}
}
