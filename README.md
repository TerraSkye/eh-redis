# EventHorizon with REDIS

## Features

- EventStore

```golang
	
    options := redis.UniversalOptions{
		Addrs:              "localhost",
		DB:                 1,
		Password:           "mypassword",
	}

    db := redis.NewUniversalClient(&options)

    defer db.close()

    store, err := ehre.NewEventStore(db)
```