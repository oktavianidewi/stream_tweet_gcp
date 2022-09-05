package go_streamer

import (
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type twitterConfig struct {
	ClientKey   string `yaml:"clientKey"`
	SecretKey   string `yaml:"secretKey"`
	AccessToken string `yaml:"accessToken"`
	TokenSecret string `yaml:"tokenSecret"`
}

type gcpConfig struct {
	ProjectId        string `yaml:"projectId"`
	TopicName        string `yaml:"topicName"`
	SubscriptionName string `yaml:"subscriptionName"`
	DatasetName      string `yaml:"datasetName"`
	TableName        string `yaml:"tableName"`
}

type Config struct {
	TwitterConfig twitterConfig `yaml:"twitter"`
	GcpConfig     gcpConfig     `yaml:"gcp"`
}

func load() *Config {
	// config path comment
	configPath := "config.yaml"
	configFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatalf("error while open config file: %v", err)
	}
	envConfig := []byte(os.ExpandEnv(string(configFile)))
	var configs Config
	err = yaml.Unmarshal(envConfig, &configs)
	if err != nil {
		log.Fatalf("failed to unmarshall configs: %v", err)
	}
	return &configs
}

func NewConfig() *Config {
	return load()
}
