// Package db connects to MongoDB.
package db

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// ConnectMongo opens a connection pool and checks it with a ping.
// The returned client is safe to share across goroutines; create it once.
func ConnectMongo(ctx context.Context, uri string) (*mongo.Client, error) {
	if uri == "" {
		return nil, fmt.Errorf("mongodb uri is required")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("connect to mongodb: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("ping mongodb: %w", err)
	}

	return client, nil
}
