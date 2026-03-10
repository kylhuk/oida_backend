package location

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	UnresolvedQueueTable    = "ops.unresolved_location_queue"
	UnresolvedStatePending  = "pending"
	UnresolvedStateResolved = "resolved"
)

type UnresolvedRecord struct {
	QueueID       string
	SubjectKind   string
	SubjectID     string
	SourceID      string
	RawID         string
	ResolverStage string
	FailureReason string
	State         string
	Priority      int
	RetryCount    int
	FirstFailedAt time.Time
	LastFailedAt  time.Time
	NextRetryAt   time.Time
	LocationHint  string
	Attrs         map[string]any
	Evidence      map[string]any
	Input         AttributionInput
	LastResult    AttributionResult
}

type ReprocessResult struct {
	QueueID string
	State   string
	Result  AttributionResult
}

type UnresolvedQueue struct {
	mu      sync.Mutex
	records map[string]UnresolvedRecord
	index   map[string]string
	order   []string
}

func NewUnresolvedQueue() *UnresolvedQueue {
	return &UnresolvedQueue{
		records: map[string]UnresolvedRecord{},
		index:   map[string]string{},
		order:   []string{},
	}
}

func (q *UnresolvedQueue) Enqueue(record UnresolvedRecord) string {
	q.mu.Lock()
	defer q.mu.Unlock()

	if record.State == "" {
		record.State = UnresolvedStatePending
	}
	if record.QueueID == "" {
		record.QueueID = fmt.Sprintf("ulq:%s:%s", record.SourceID, record.SubjectID)
	}
	if record.FirstFailedAt.IsZero() {
		record.FirstFailedAt = time.Now().UTC()
	}
	if record.LastFailedAt.IsZero() {
		record.LastFailedAt = record.FirstFailedAt
	}
	if record.NextRetryAt.IsZero() {
		record.NextRetryAt = record.LastFailedAt
	}

	key := record.SourceID + "|" + record.SubjectID
	if queueID, exists := q.index[key]; exists {
		existing := q.records[queueID]
		record.QueueID = queueID
		record.FirstFailedAt = existing.FirstFailedAt
		record.RetryCount = existing.RetryCount
	}

	q.records[record.QueueID] = record
	if q.index[key] == "" {
		q.index[key] = record.QueueID
		q.order = append(q.order, record.QueueID)
	}
	return record.QueueID
}

func (q *UnresolvedQueue) Pending() []UnresolvedRecord {
	q.mu.Lock()
	defer q.mu.Unlock()

	items := make([]UnresolvedRecord, 0, len(q.records))
	for _, queueID := range q.order {
		record := q.records[queueID]
		if record.State == UnresolvedStatePending {
			items = append(items, record)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Priority != items[j].Priority {
			return items[i].Priority > items[j].Priority
		}
		if !items[i].NextRetryAt.Equal(items[j].NextRetryAt) {
			return items[i].NextRetryAt.Before(items[j].NextRetryAt)
		}
		return items[i].QueueID < items[j].QueueID
	})
	return items
}

func (q *UnresolvedQueue) CountPending() int {
	return len(q.Pending())
}

func (q *UnresolvedQueue) Reprocess(attributor *Attributor, mutate func(*AttributionInput)) []ReprocessResult {
	if attributor == nil {
		return nil
	}
	now := time.Now().UTC()
	if attributor.Now != nil {
		now = attributor.Now().UTC()
	}

	q.mu.Lock()
	recordIDs := make([]string, 0, len(q.order))
	for _, queueID := range q.order {
		record := q.records[queueID]
		if record.State != UnresolvedStatePending {
			continue
		}
		if record.NextRetryAt.After(now) {
			continue
		}
		recordIDs = append(recordIDs, queueID)
	}
	q.mu.Unlock()

	results := make([]ReprocessResult, 0, len(recordIDs))
	for _, queueID := range recordIDs {
		q.mu.Lock()
		record := q.records[queueID]
		input := record.Input
		q.mu.Unlock()

		if mutate != nil {
			mutate(&input)
		}

		result := attributor.attribute(input, false)

		q.mu.Lock()
		updated := q.records[queueID]
		updated.LastResult = result
		updated.LastFailedAt = now
		updated.Input = input
		if result.Resolved {
			updated.State = UnresolvedStateResolved
			updated.FailureReason = ""
		} else {
			updated.RetryCount++
			updated.NextRetryAt = now.Add(backoffDuration(updated.RetryCount))
			if result.GeoConfidence > 0 {
				updated.FailureReason = UnresolvedReasonLowConfidence
			} else {
				updated.FailureReason = UnresolvedReasonNoMatch
			}
		}
		q.records[queueID] = updated
		q.mu.Unlock()

		results = append(results, ReprocessResult{QueueID: queueID, State: updated.State, Result: result})
	}

	return results
}

func backoffDuration(retry int) time.Duration {
	if retry <= 0 {
		return 0
	}
	minutes := retry * retry
	if minutes > 360 {
		minutes = 360
	}
	return time.Duration(minutes) * time.Minute
}
