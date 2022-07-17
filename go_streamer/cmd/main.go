package main

import (
	go_streamer "github.com/oktavianidewi/stream_tweet_gcp"
)

// publisher
func main() {
	config := go_streamer.NewConfig()
	publisher := go_streamer.NewPublisher(config.GcpConfig)
	streaming := go_streamer.NewStreamer(config.TwitterConfig, publisher)
	streaming.Start()
}
