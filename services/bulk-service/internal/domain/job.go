// Package domain holds bulk job types.
package domain

import (
	"context"
	"errors"
	"time"
)

type Status string

const (
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusCompleted Status = "COMPLETED"
)

// Outcome of one item in a job.
type Outcome int

const (
	OutcomeValid Outcome = iota
	OutcomeInvalid
	OutcomeFailed // no source could answer; the client can resubmit these
)

var ErrNotFound = errors.New("job not found")

// Job tracks progress with counters, so polling a 100,000-item job is one
// small read, not a scan over every item.
type Job struct {
	ID          string     `bson:"_id"`
	ClientID    string     `bson:"client_id"`
	Status      Status     `bson:"status"`
	Total       int        `bson:"total"`
	Processed   int        `bson:"processed"`
	Valid       int        `bson:"valid"`
	Invalid     int        `bson:"invalid"`
	Failed      int        `bson:"failed"`
	CreatedAt   time.Time  `bson:"created_at"`
	CompletedAt *time.Time `bson:"completed_at,omitempty"`
}

// Item is one PAN to verify. Items live only in memory while queued, so
// raw PANs are never written to the bulk service's database.
type Item struct {
	JobID       string
	ClientID    string
	Row         int
	PAN         string
	Name        string
	ReferenceID string
}

type Repository interface {
	Create(ctx context.Context, j *Job) error
	Get(ctx context.Context, clientID, id string) (*Job, error)
	// Record atomically counts one finished item and returns the updated job.
	Record(ctx context.Context, jobID string, o Outcome) (*Job, error)
	MarkCompleted(ctx context.Context, jobID string, at time.Time) error
}
