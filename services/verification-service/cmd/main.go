package main

import (
	"context"
	"log"
	"net"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"kyc-platform/services/verification-service/internal/cache"
	"kyc-platform/services/verification-service/internal/grpcapi"
	"kyc-platform/services/verification-service/internal/orchestrator"
	"kyc-platform/services/verification-service/internal/publisher"
	"kyc-platform/services/verification-service/internal/repository"
	"kyc-platform/services/verification-service/internal/service"
	"kyc-platform/services/verification-service/internal/source"
	"kyc-platform/shared/contracts"
	"kyc-platform/shared/db"
	"kyc-platform/shared/env"
	"kyc-platform/shared/events"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// ctx is cancelled on Ctrl+C or SIGTERM (e.g. Kubernetes stopping the pod).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var (
		grpcAddr        = env.GetString("GRPC_ADDR", ":9093")
		mongoURI        = env.GetString("MONGODB_URI", "mongodb://localhost:27018")
		mongoDB         = env.GetString("MONGODB_DATABASE", "verification")
		redisAddr       = env.GetString("REDIS_ADDR", "localhost:6379")
		piiSecret       = env.GetString("PII_SECRET", "")
		sourceAURL      = env.GetString("SOURCE_A_URL", "http://localhost:8090/sources/source-a/pan")
		sourceBURL      = env.GetString("SOURCE_B_URL", "http://localhost:8090/sources/source-b/pan")
		sourceTimeout   = env.GetDuration("SOURCE_TIMEOUT", 800*time.Millisecond)
		breakerFailures = env.GetInt("BREAKER_FAILURE_THRESHOLD", 5)
		breakerOpenFor  = env.GetDuration("BREAKER_OPEN_TIMEOUT", 30*time.Second)
		cacheTTL        = env.GetDuration("CACHE_TTL", 24*time.Hour)
		kafkaBrokers    = strings.Split(env.GetString("KAFKA_BROKERS", "localhost:9094"), ",")
	)

	if piiSecret == "" {
		log.Fatal("PII_SECRET is required")
	}

	// Storage
	mongoClient, err := db.ConnectMongo(ctx, mongoURI)
	if err != nil {
		log.Fatal(err)
	}
	defer mongoClient.Disconnect(context.Background())

	repo, err := repository.NewMongoRepository(ctx, mongoClient.Database(mongoDB))
	if err != nil {
		log.Fatal(err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	// Upstream sources, primary first
	orch := orchestrator.New([]source.Source{
		source.NewHTTPSource("source-a", sourceAURL, sourceTimeout),
		source.NewHTTPSource("source-b", sourceBURL, sourceTimeout),
	}, breakerFailures, breakerOpenFor)

	// Events
	if err := events.EnsureTopics(kafkaBrokers, 3, contracts.TopicVerificationCompleted); err != nil {
		log.Printf("ensure kafka topics: %v", err)
	}
	producer := events.NewProducer(kafkaBrokers)
	defer producer.Close()

	// Wire everything together: this is our dependency injection.
	svc := service.New(repo, cache.NewRedisCache(redisClient, cacheTTL), orch, publisher.NewKafkaPublisher(producer), piiSecret)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", grpcAddr, err)
	}

	server := grpc.NewServer()
	grpcapi.Register(server, svc)
	reflection.Register(server) // lets tools like grpcurl discover the API

	go func() {
		log.Printf("verification service listening on %s", grpcAddr)
		if err := server.Serve(lis); err != nil {
			log.Printf("grpc server stopped: %v", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Println("shutting down verification service")
	server.GracefulStop() // finish in-flight calls, refuse new ones
}
