package provider

import (
	"testing"

	"github.com/open-policy-agent/gatekeeper/v3/pkg/pubsub/dapr"
	"github.com/open-policy-agent/gatekeeper/v3/pkg/pubsub/rabbitmq"
)

func Test_newPubSubSet(t *testing.T) {
	tests := []struct {
		name     string
		pubSubs  map[string]InitiateConnection
		wantKeys []string
	}{
		{
			name: "multiple providers are available",
			pubSubs: map[string]InitiateConnection{
				dapr.Name:     dapr.NewConnection,
				rabbitmq.Name: rabbitmq.NewConnection,
			},
			wantKeys: []string{dapr.Name, rabbitmq.Name},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newPubSubSet(tt.pubSubs)
			for _, wantKey := range tt.wantKeys {
				if _, ok := got.supportedPubSub[wantKey]; !ok {
					t.Errorf("newPubSubSet() = %#v, want key %#v", got.supportedPubSub, wantKey)
				}
			}

		})
	}
}

func TestList(t *testing.T) {
	tests := []struct {
		name    string
		wantKey string
	}{
		{
			name:    "only one provider is available",
			wantKey: dapr.Name,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := List()
			if _, ok := got[tt.wantKey]; !ok {
				t.Errorf("List() = %#v, want key %#v", got, tt.wantKey)
			}
		})
	}
}
