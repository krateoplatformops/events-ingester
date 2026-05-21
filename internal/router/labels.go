package router

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/krateoplatformops/events-ingester/internal/objects"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
)

const (
	keyCompositionID = "krateo.io/composition-id"
	cacheTTL         = 5 * time.Minute
)

var ErrCompositionIdNotFound = errors.New("composition id NOT found")

type cacheEntry struct {
	value     string
	expiresAt time.Time
}

var compositionCache sync.Map

func hasCompositionId(obj *corev1.Event) bool {
	labels := obj.GetLabels()
	if len(labels) == 0 {
		return false
	}

	val, ok := labels[keyCompositionID]
	if len(val) == 0 {
		return false
	}
	return ok
}

func findCompositionID(resolver *objects.ObjectResolver, ref *corev1.ObjectReference, log *slog.Logger) (cid string, err error) {
	// 1. Hit cache
	if entryI, ok := compositionCache.Load(ref.UID); ok {
		entry := entryI.(cacheEntry)
		if time.Now().Before(entry.expiresAt) {
			return entry.value, nil
		}
		// expired, remove
		compositionCache.Delete(ref.UID)
	}

	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return "", err
	}

	// 2. Check if InvolvedObject is a Composition
	if gv.Group == "composition.krateo.io" {
		cid = string(ref.UID)
	}

	// 3. Hit k8s API server
	if cid == "" {
		var obj *unstructured.Unstructured
		retryErr := retry.OnError(retry.DefaultRetry,
			func(e error) bool {
				if e != nil {
					resolver.InvalidateRESTMapperCache()
					return true
				}
				return false
			},
			func() error {
				obj, err = resolver.ResolveReference(context.Background(), ref)
				return err
			})
		if retryErr != nil {
			return "", retryErr
		}

		if obj == nil {
			log.Warn("object not found resolving reference",
				slog.String("name", ref.Name),
				slog.String("kind", ref.Kind),
				slog.String("apiVersion", ref.APIVersion),
			)
			return "", nil
		}

		labels := obj.GetLabels()
		if len(labels) == 0 {
			log.Info("no labels found in resolved reference",
				slog.String("name", ref.Name),
				slog.String("kind", ref.Kind),
				slog.String("apiVersion", ref.APIVersion),
			)
			return "", nil
		}

		var ok bool
		cid, ok = labels[keyCompositionID]
		if !ok || cid == "" {
			return "", ErrCompositionIdNotFound
		}
	}

	// 4. Update cache
	compositionCache.Store(ref.UID, cacheEntry{
		value:     cid,
		expiresAt: time.Now().Add(cacheTTL),
	})

	return cid, nil
}

func StartCacheCleaner(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return // exit goroutine
			case <-ticker.C:
				now := time.Now()
				compositionCache.Range(func(key, val interface{}) bool {
					entry := val.(cacheEntry)
					if now.After(entry.expiresAt) {
						compositionCache.Delete(key)
					}
					return true
				})
			}
		}
	}()
}
