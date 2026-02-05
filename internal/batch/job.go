package batch

import (
	"github.com/krateoplatformops/events-ingester/internal/queue"
)

var _ queue.Jober = (*InsertRecordJob)(nil)

// InsertRecordJob is a thin wrapper that pushes a record into a channel for the batch worker.
type InsertRecordJob struct {
	Record InsertRecord
	Input  chan<- InsertRecord
}

func (j *InsertRecordJob) Job() {
	j.Input <- j.Record
}
