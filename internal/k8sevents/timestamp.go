package k8sevents

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Timestamp returns the canonical event timestamp used by the router and DB writes.
func Timestamp(evt corev1.Event) time.Time {
	ts := evt.EventTime.Time
	if ts.IsZero() {
		ts = evt.LastTimestamp.Time
	}
	if ts.IsZero() {
		return time.Time{}
	}
	return ts.UTC()
}
