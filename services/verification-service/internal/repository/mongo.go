package repository

import (
	"context"
	"errors"
	"fmt"

	"kyc-platform/services/verification-service/internal/domain"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const verificationsCollection = "verifications"

type MongoRepository struct {
	coll *mongo.Collection
}

// NewMongoRepository also creates the indexes the queries rely on.
// Creating an index that already exists is a no-op, so this is safe on every start.
func NewMongoRepository(ctx context.Context, db *mongo.Database) (*MongoRepository, error) {
	coll := db.Collection(verificationsCollection)

	_, err := coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			// Idempotency guarantee: one record per (client, reference ID).
			// Unique at the database level, so two concurrent requests with the
			// same reference ID can't both be saved.
			Keys: bson.D{{Key: "client_id", Value: 1}, {Key: "reference_id", Value: 1}},
			Options: options.Index().
				SetUnique(true).
				SetPartialFilterExpression(bson.M{"reference_id": bson.M{"$exists": true}}),
		},
		{
			// Audit queries: "all checks for this PAN", without storing the PAN.
			Keys: bson.D{{Key: "pan_fingerprint", Value: 1}, {Key: "created_at", Value: -1}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create indexes: %w", err)
	}

	return &MongoRepository{coll: coll}, nil
}

func (r *MongoRepository) Save(ctx context.Context, v *domain.Verification) error {
	_, err := r.coll.InsertOne(ctx, v)
	if mongo.IsDuplicateKeyError(err) {
		return domain.ErrDuplicateReference
	}
	return err
}

func (r *MongoRepository) GetByID(ctx context.Context, clientID, id string) (*domain.Verification, error) {
	// Filtering by client_id too means a client can't read another client's
	// record even if it guesses the ID.
	return r.findOne(ctx, bson.M{"_id": id, "client_id": clientID})
}

func (r *MongoRepository) GetByReference(ctx context.Context, clientID, referenceID string) (*domain.Verification, error) {
	return r.findOne(ctx, bson.M{"client_id": clientID, "reference_id": referenceID})
}

func (r *MongoRepository) findOne(ctx context.Context, filter bson.M) (*domain.Verification, error) {
	var v domain.Verification
	err := r.coll.FindOne(ctx, filter).Decode(&v)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}
