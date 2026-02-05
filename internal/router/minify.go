package router

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func minifyEvent(e corev1.Event) corev1.Event {
	out := corev1.Event{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Event",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.ObjectMeta.Name,
			Namespace: e.ObjectMeta.Namespace,
			Labels:    e.ObjectMeta.Labels,
		},
		Reason:  e.Reason,
		Message: e.Message,
		Type:    e.Type,
	}

	out.EventTime = e.EventTime
	out.FirstTimestamp = e.FirstTimestamp
	out.LastTimestamp = e.LastTimestamp

	// Source vuoto
	out.Source = corev1.EventSource{}

	// InvolvedObject leggero
	out.InvolvedObject = corev1.ObjectReference{
		Kind:      e.InvolvedObject.Kind,
		Namespace: e.InvolvedObject.Namespace,
		Name:      e.InvolvedObject.Name,
		UID:       e.InvolvedObject.UID,
	}

	// Rimuovi campi rumorosi
	out.ReportingController = ""
	out.ReportingInstance = ""
	out.Action = ""
	out.ManagedFields = nil

	return out
}
