package go_streamer

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

type StreamManager struct {
	config    twitterConfig
	client    *http.Client
	publisher *PublishManager
}

func (s *StreamManager) Start() {

	// Twitter Client
	client := twitter.NewClient(s.client)

	// Convenience Demux demultiplexed stream messages
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		// publish to pubsub

		fmt.Println(tweet)
		// convert tweet to string
		// s.publisher.Write(tweet)

	}

	fmt.Println("Starting Stream...")

	// FILTER
	filterParams := &twitter.StreamFilterParams{
		Track:         []string{"covid"},
		Language:      []string{"en"},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(filterParams)
	if err != nil {
		log.Fatal(err)
	}

	// Receive messages until stopped or stream quits
	go demux.HandleChan(stream.Messages)

	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	fmt.Println("Stopping Stream...")
	stream.Stop()

}

func NewStreamer(tConfig twitterConfig, publisher *PublishManager) *StreamManager {
	config := oauth1.NewConfig(tConfig.ClientKey, tConfig.SecretKey)
	token := oauth1.NewToken(tConfig.AccessToken, tConfig.TokenSecret)
	httpClient := config.Client(oauth1.NoContext, token)

	return &StreamManager{
		config:    tConfig,
		client:    httpClient,
		publisher: publisher,
	}
}
