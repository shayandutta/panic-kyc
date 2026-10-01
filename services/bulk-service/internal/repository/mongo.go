package repository

import (
	"context"
	"errors"
	"time"

	"kyc-platform/services/bulk-service/internal/domain"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoRepository struct {
	coll *mongo.Collection
}

func NewMongoRepository(db *mongo.Database) *MongoRepository {
	return &MongoRepository{coll: db.Collection("bulk_jobs")}
}

func (r *MongoRepository) Create(ctx context.Context, j *domain.Job) error {
	_, err := r.coll.InsertOne(ctx, j)
	return err
}

func (r *MongoRepository) Get(ctx context.Context, clientID, id string) (*domain.Job, error) {
	var j domain.Job
	err := r.coll.FindOne(ctx, bson.M{"_id": id, "client_id": clientID}).Decode(&j)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, domain.ErrNotFound
	}
	return &j, err
}

// Record uses $inc, which MongoDB applies atomically on the server. Many
// workers can finish items of the same job at once without losing counts,
// the database version of the mutex-protected counter.
func (r *MongoRepository) Record(ctx context.Context, jobID string, o domain.Outcome) (*domain.Job, error) {
	field := map[domain.Outcome]string{
		domain.OutcomeValid:   "valid",
		domain.OutcomeInvalid: "invalid",
		domain.OutcomeFailed:  "failed",
	}[o]

	var j domain.Job
	err := r.coll.FindOneAndUpdate(ctx,
		bson.M{"_id": jobID},
		bson.M{
			"$inc": bson.M{"processed": 1, field: 1},
			"$set": bson.M{"status": domain.StatusRunning},
		},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&j)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, domain.ErrNotFound
	}
	return &j, err
}

func (r *MongoRepository) MarkCompleted(ctx context.Context, jobID string, at time.Time) error {
	_, err := r.coll.UpdateOne(ctx,
		bson.M{"_id": jobID},
		bson.M{"$set": bson.M{"status": domain.StatusCompleted, "completed_at": at}},
	)
	return err
}
