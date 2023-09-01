package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/open-policy-agent/gatekeeper/v3/pkg/pubsub/connection"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const Name = "rabbitmq"

type ClientConfig struct {
	// ConnectionString for the rabbitmq instance
	ConnectionString string `json:"connectionString"`

	// TopicConfig config for the topics
	TopicConfig `json:"topicConfig"`
}

type TopicConfig struct {
	// MaxAge retention policy for the stream. Must be parseable by time.ParseDuration
	MaxAge string `json:"maxAge"`

	// MaxLengthBytes retention policy for the stream. Supported formats (kb|mb|gb|tb)
	MaxLengthBytes string `json:"maxLengthBytes"`
}

// RabbitMQConnection represents driver for interacting with pub sub using rabbitmq.
type RabbitMQConnection struct {
	environment *stream.Environment
	mu          sync.Mutex
	producers   map[string]*stream.Producer
	topicConfig TopicConfig
}

func (r *RabbitMQConnection) Publish(_ context.Context, data interface{}, topic string) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling data: %w", err)
	}

	producer, err := r.getProducer(topic)
	if err != nil {
		return fmt.Errorf("error getting producer: %w", err)
	}

	err = producer.Send(amqp.NewMessage(jsonData))
	if err != nil {
		return fmt.Errorf("error publishing message to rabbitmq: %w", err)
	}

	return nil
}

func (r *RabbitMQConnection) getProducer(topic string) (*stream.Producer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	duration, err := time.ParseDuration(r.topicConfig.MaxAge)
	if err != nil {
		return nil, err
	}
	if _, ok := r.producers[topic]; !ok {
		err := r.environment.DeclareStream(topic,
			stream.NewStreamOptions().
				SetMaxAge(duration).
				SetMaxLengthBytes(stream.ByteCapacity{}.From(r.topicConfig.MaxLengthBytes)))
		if err != nil {
			return nil, fmt.Errorf("error creating stream: %w", err)
		}

		p, err := r.environment.NewProducer(topic, nil)
		if err != nil {
			return nil, fmt.Errorf("error creating producer: %w", err)
		}

		r.producers[topic] = p
	}
	return r.producers[topic], nil
}

func (r *RabbitMQConnection) CloseConnection() error {
	return r.environment.Close()
}

func (r *RabbitMQConnection) UpdateConnection(_ context.Context, config interface{}) error {
	var clientConfig *ClientConfig
	cfg, err := extractConfig(config)
	if err != nil {
		return err
	}
	clientConfig = cfg

	r.mu.Lock()
	defer r.mu.Unlock()

	r.topicConfig = TopicConfig{
		MaxAge:         clientConfig.TopicConfig.MaxAge,
		MaxLengthBytes: clientConfig.TopicConfig.MaxLengthBytes,
	}

	r.producers = map[string]*stream.Producer{}

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUri(clientConfig.ConnectionString))
	if err != nil {
		return err
	}
	r.environment = env

	return nil
}

// Returns a new client for rabbitmq.
func NewConnection(_ context.Context, config interface{}) (connection.Connection, error) {
	var clientConfig *ClientConfig
	cfg, err := extractConfig(config)
	if err != nil {
		return nil, err
	}
	clientConfig = cfg

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUri(clientConfig.ConnectionString))
	if err != nil {
		return nil, err
	}

	return &RabbitMQConnection{
		environment: env,
		producers:   map[string]*stream.Producer{},
		topicConfig: TopicConfig{
			MaxAge:         clientConfig.TopicConfig.MaxAge,
			MaxLengthBytes: clientConfig.TopicConfig.MaxLengthBytes,
		},
	}, nil
}

func extractConfig(config interface{}) (*ClientConfig, error) {
	cfg := ClientConfig{TopicConfig: TopicConfig{}}

	m, ok := config.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid type assertion, config is not in expected format")
	}

	cfg.ConnectionString, ok = m["connectionString"].(string)
	if !ok {
		return nil, fmt.Errorf("failed to get value of connectionString")
	}

	cfg.TopicConfig.MaxAge, ok = m["maxAge"].(string)
	if !ok {
		return nil, fmt.Errorf("failed to get value of maxAge")
	}

	cfg.TopicConfig.MaxLengthBytes, ok = m["maxLengthBytes"].(string)
	if !ok {
		return nil, fmt.Errorf("failed to get value of maxLengthBytes")
	}

	return &cfg, nil
}
