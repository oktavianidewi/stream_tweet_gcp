package go_streamer

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

type PublishManager struct {
	ctx    context.Context
	client *pubsub.Client
	config gcpConfig
}

// NewPublisher func to publish message to kafka
func NewPublisher(gConfig gcpConfig) *PublishManager {
	ctx := context.Background()
	pubsubClient, err := pubsub.NewClient(ctx, gConfig.ProjectId)
	if err != nil {
		log.Fatal(err)
	}
	return &PublishManager{
		ctx:    ctx,
		client: pubsubClient,
		config: gConfig,
	}
}

func (p *PublishManager) Write(messages []byte) {

	topic := p.client.Topic(p.config.TopicName)
	result := topic.Publish(p.ctx, &pubsub.Message{
		Data: messages,
	})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(p.ctx)
	if err != nil {
		log.Fatalf("pubsub: result.Get: %v", err)
	}
	fmt.Printf("Published a message; msg ID: %v\n", id)
	// defer p.client.Close()
	return
}
