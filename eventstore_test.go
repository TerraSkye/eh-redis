package ehpg_test

import (
	"context"
	"github.com/go-redis/redis"
	testsuite "github.com/looplab/eventhorizon/eventstore"
	"github.com/looplab/eventhorizon/namespace"
	rediseventstore "github.com/terraskye/eh-redis"
	"testing"
)

func TestEventStore(t *testing.T) {

	options := redis.UniversalOptions{
		Addrs:     []string{"127.0.0.1:6379"},
		DB:        0,
		OnConnect: nil,
		Password:  "",
	}
	db := redis.NewUniversalClient(&options)

	defer db.Close()

	store, err := rediseventstore.NewEventStore(db)
	if err != nil {
		t.Fatal("there should be no error")
	}
	if store == nil {
		t.Fatal("there should be a store")
	}

	ctx := namespace.NewContext(context.Background(), "ns")

	_ = store.Clear(context.Background())

	defer func() {
		t.Log("clearing db")
		if err := store.Clear(context.Background()); err != nil {
			t.Fatal("there should be no error:", err)
		}
		if err := store.Clear(ctx); err != nil {
			t.Fatal("there should be no error:", err)
		}
	}()

	// Run the actual test suite.
	t.Log("event store with default namespace")
	testsuite.AcceptanceTest(t, store, ctx)

	t.Log("event store with other namespace")
	testsuite.AcceptanceTest(t, store, namespace.NewContext(context.Background(), "other"))

}
