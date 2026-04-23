package k8sevents

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTimestampUsesEventTimeThenLastTimestampAndNormalizesUTC(t *testing.T) {
	last := time.Date(2026, 4, 23, 8, 0, 0, 0, time.FixedZone("CET", 2*60*60))
	eventTime := time.Date(2026, 4, 23, 10, 30, 0, 123000000, time.FixedZone("EEST", 3*60*60))

	evt := corev1.Event{
		EventTime:     metav1.MicroTime{Time: eventTime},
		LastTimestamp: metav1.Time{Time: last},
	}

	got := Timestamp(evt)
	want := eventTime.UTC()
	if !got.Equal(want) {
		t.Fatalf("expected %s, got %s", want, got)
	}

	evt.EventTime = metav1.MicroTime{}
	got = Timestamp(evt)
	want = last.UTC()
	if !got.Equal(want) {
		t.Fatalf("expected fallback %s, got %s", want, got)
	}
}
