package go_streamer

import (
	"encoding/json"
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

type ExtractTweet struct {
	CreatedAt         string     `json:"created_at,omitempty"`
	Id                int64      `json:"id,omitempty"`
	PossiblySensitive bool       `json:"possibly_sensitive,omitempty"`
	QuoteCount        int32      `json:"quote_count,omitempty"`
	ReplyCount        int32      `json:"reply_count,omitempty"`
	Retweeted         bool       `json:"retweeted,omitempty"`
	RetweetCount      int32      `json:"retweet_count,omitempty"`
	Source            string     `json:"source,omitempty"`
	Text              string     `json:"text,omitempty"`
	Lang              string     `json:"lang,omitempty"`
	User              UserStruct `json:"user,omitempty"`
}

type UserStruct struct {
	CreatedAt      string `json:"created_at,omitempty"`
	FollowersCount int32  `json:"followers_count,omitempty"`
	FriendsCount   int32  `json:"friends_count,omitempty"`
	Id             int64  `json:"id,omitempty"`
	Location       string `json:"location,omitempty"`
	Name           string `json:"name,omitempty"`
	Protected      bool   `json:"protected,omitempty"`
	ScreenName     string `json:"screen_name,omitempty"`
}

func (s *StreamManager) Parsing(tweet *twitter.Tweet) []byte {

	// select field
	tweets := ExtractTweet{
		CreatedAt:         tweet.CreatedAt,
		Id:                tweet.ID,
		PossiblySensitive: tweet.PossiblySensitive,
		QuoteCount:        int32(tweet.QuoteCount),
		ReplyCount:        int32(tweet.ReplyCount),
		Retweeted:         tweet.Retweeted,
		RetweetCount:      int32(tweet.RetweetCount),
		Source:            tweet.Source,
		Text:              tweet.Text,
		Lang:              tweet.Lang,
		User: UserStruct{
			CreatedAt:      tweet.User.CreatedAt,
			FollowersCount: int32(tweet.User.FollowersCount),
			FriendsCount:   int32(tweet.User.FriendsCount),
			Id:             tweet.User.ID,
			Location:       tweet.User.Location,
			Name:           tweet.User.Name,
			Protected:      tweet.User.Protected,
			ScreenName:     tweet.User.ScreenName,
		},
	}

	byteTweets, err := json.Marshal(tweets)
	if err != nil {
		log.Fatal(err)
	}

	return byteTweets
}

func (s *StreamManager) Start() {
	// Twitter Client
	client := twitter.NewClient(s.client)

	// Convenience Demux demultiplexed stream messages
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		// parse tweet
		parsedTweet := s.Parsing(tweet)

		// publish to pubsub
		s.publisher.Write(parsedTweet)
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
