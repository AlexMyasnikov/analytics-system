package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"analytics-system/producer/graph/generated"
	"analytics-system/producer/graph/model"
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

func (r *mutationResolver) RegisterKafkaEvent(ctx context.Context, event model.RegisterKafkaEventInput) (*model.Event, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message t0 %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	topic := event.EventType
	CreateTopic(topic)
	currentTimeStamp := fmt.Sprintf("%v", time.Now().Unix())

	e := model.Event{
		ID: currentTimeStamp,
		EventType: &event.EventType,
		Path: &event.Path,
		Search: &event.Search,
		Title: &event.Title,
		UserID: &event.UserID,
		URL: &event.URL,
	}
	value, err := json.Marshal(e)
	if err != nil {
		log.Println("=> error converting event object to bytes:", err)
	}
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
	}, nil)
	if err != nil {
		return nil, err
	}

	p.Flush(15 * 1000)

	return &e, nil
}

func (r *queryResolver) Ping(ctx context.Context) (*model.PingResponse, error) {
	res := &model.PingResponse{
		Message: "Hello, world",
	}
	return res, nil
}

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }

func CreateTopic(topicName string)  {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	defer a.Close()

	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	results, err := a.CreateTopics(
		ctx, []kafka.TopicSpecification{{
			Topic: topicName,
			NumPartitions: 1,
		}},
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		log.Printf("Failed to create topic: %v\n", err)
	}
	log.Println("results", results)
}