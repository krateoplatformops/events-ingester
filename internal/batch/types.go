package batch

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type InsertRecord struct {
	CreatedAt       time.Time
	ClusterName     string
	UID             string
	GlobalUID       string
	Namespace       string
	ResourceKind    string
	ResourceName    string
	EventType       string
	Reason          string
	Message         string
	CompositionID   pgtype.UUID
	Raw             []byte
	ResourceVersion string
}
