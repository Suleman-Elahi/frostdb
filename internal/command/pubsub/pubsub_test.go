package pubsub

import (
	"io"
	"net"
	"testing"

	"frostdb/internal/engine/api"
	"frostdb/internal/engine/core"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestPublishNoSubscribers(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	pub := PublishCommand{}
	res := pub.Execute(ctx, []types.Value{
		types.NewString("channel1"),
		types.NewString("hello"),
	})
	if res.Type != types.TypeInteger || res.Num != 0 {
		t.Fatalf("expected 0 subscribers, got %v", res)
	}
}

func TestPublishWithSubscriber(t *testing.T) {
	engine := core.New()

	// Subscribe from engine directly to test publish count
	ch := make(chan api.PubSubMessage, 10)
	cleanup := engine.PubSubSubscribe("channel1", ch)
	defer cleanup()

	ctx := &context.Context{Engine: engine}
	pub := PublishCommand{}
	res := pub.Execute(ctx, []types.Value{
		types.NewString("channel1"),
		types.NewString("hello"),
	})
	if res.Type != types.TypeInteger || res.Num != 1 {
		t.Fatalf("expected 1 subscriber, got %v", res)
	}

	// Verify message was received
	msg := <-ch
	if msg.Channel != "channel1" || msg.Message != "hello" {
		t.Fatalf("unexpected message: %+v", msg)
	}
}

func TestPublishMultipleSubscribers(t *testing.T) {
	engine := core.New()

	ch1 := make(chan api.PubSubMessage, 10)
	cleanup1 := engine.PubSubSubscribe("ch", ch1)
	defer cleanup1()
	ch2 := make(chan api.PubSubMessage, 10)
	cleanup2 := engine.PubSubSubscribe("ch", ch2)
	defer cleanup2()

	ctx := &context.Context{Engine: engine}
	pub := PublishCommand{}
	res := pub.Execute(ctx, []types.Value{
		types.NewString("ch"),
		types.NewString("msg"),
	})
	if res.Num != 2 {
		t.Fatalf("expected 2 subscribers, got %d", res.Num)
	}
}

func TestUnsubscribeNoSubs(t *testing.T) {
	engine := core.New()
	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close()
	go io.Copy(io.Discard, p2)
	ctx := &context.Context{Engine: engine, Conn: p1}

	unsub := UnsubscribeCommand{}
	res := unsub.Execute(ctx, nil)
	if res.Type != types.TypeNoReply {
		t.Fatalf("expected NoReply, got %v", res)
	}
}
