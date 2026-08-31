package metrics

import "sync/atomic"

type Counters struct {
	Enqueued  atomic.Int64
	Delivered atomic.Int64
	Acked     atomic.Int64
	Nacked    atomic.Int64
	Reclaimed atomic.Int64
	Retried   atomic.Int64
	Buried    atomic.Int64
	Requeued  atomic.Int64
}

type Snapshot struct {
	Enqueued  int64 `json:"enqueued"`
	Delivered int64 `json:"delivered"`
	Acked     int64 `json:"acked"`
	Nacked    int64 `json:"nacked"`
	Reclaimed int64 `json:"reclaimed"`
	Retried   int64 `json:"retried"`
	Buried    int64 `json:"buried"`
	Requeued  int64 `json:"requeued"`
}

func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		Enqueued:  c.Enqueued.Load(),
		Delivered: c.Delivered.Load(),
		Acked:     c.Acked.Load(),
		Nacked:    c.Nacked.Load(),
		Reclaimed: c.Reclaimed.Load(),
		Retried:   c.Retried.Load(),
		Buried:    c.Buried.Load(),
		Requeued:  c.Requeued.Load(),
	}
}
