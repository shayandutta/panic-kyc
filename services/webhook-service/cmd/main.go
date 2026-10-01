package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"kyc-platform/services/webhook-service/internal/delivery"
	"kyc-platform/services/webhook-service/internal/dispatcher"
	"kyc-platform/shared/contracts"
	"kyc-platform/shared/env"
	"kyc-platform/shared/events"
)

const consumerGroup = "webhook-service"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var (
		brokers         = strings.Split(env.GetString("KAFKA_BROKERS", "localhost:9094"), ",")
		webhooksFile    = env.GetString("WEBHOOKS_FILE", "deploy/dev/webhooks.json")
		consumers       = env.GetInt("CONSUMERS", 3)
		deliveryTimeout = env.GetDuration("DELIVERY_TIMEOUT", 5*time.Second)
		retryBase       = env.GetDuration("RETRY_BASE_DELAY", 2*time.Second)
		maxAttempts     = env.GetInt("MAX_ATTEMPTS", 6)
	)

	endpoints, err := loadEndpoints(webhooksFile)
	if err != nil {
		log.Fatal(err)
	}

	if err := events.EnsureTopics(brokers, 3,
		contracts.TopicVerificationCompleted, contracts.TopicWebhookRetry, contracts.TopicWebhookDLQ); err != nil {
		log.Printf("ensure kafka topics: %v", err)
	}

	producer := events.NewProducer(brokers)
	defer producer.Close()

	policy := delivery.RetryPolicy{MaxAttempts: maxAttempts, Base: retryBase, Max: 10 * time.Minute}
	d := dispatcher.New(endpoints, delivery.NewSender(deliveryTimeout), policy, producer)

	// Each consumer joins the same group, so Kafka splits partitions between
	// them. More consumers = more parallel deliveries, up to the partition
	// count. Across machines, the same knob is the number of pods.
	var wg sync.WaitGroup
	for i := 0; i < consumers; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := events.Consume(ctx, brokers, contracts.TopicVerificationCompleted, consumerGroup, d.HandleEvent); err != nil {
				log.Printf("event consumer stopped: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := events.Consume(ctx, brokers, contracts.TopicWebhookRetry, consumerGroup+"-retry", d.HandleRetry); err != nil {
				log.Printf("retry consumer stopped: %v", err)
			}
		}()
	}

	log.Printf("webhook service running with %d consumers per topic", consumers)
	<-ctx.Done()
	log.Println("shutting down webhook service, finishing in-flight deliveries")
	wg.Wait()
}

func loadEndpoints(path string) ([]delivery.Endpoint, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var eps []delivery.Endpoint
	return eps, json.Unmarshal(raw, &eps)
}
