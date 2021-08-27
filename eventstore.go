package ehpg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/namespace"
	"sort"
	"strconv"
	"time"
)

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

// ErrConflictVersion is when a version conflict occurs when saving an aggregate.
var ErrVersionConflict = errors.New("can not create/update aggregate")

// ErrCouldNotMarshalEvent is when an event could not be marshaled into JSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshalled into a concrete type.
var ErrCouldNotUnmarshalEvent = errors.New("could not unmarshal event")

// ErrCouldNotSaveAggregate is when an aggregate could not be saved.
var ErrCouldNotSaveAggregate = errors.New("could not save aggregate")

// EventStore implements an eh.EventStore for PostgreSQL.
type EventStore struct {
	db      redis.UniversalClient
	encoder Encoder
}

var _ = eh.EventStore(&EventStore{})

type AggregateRecord struct {
	Namespace   string
	AggregateID uuid.UUID
	Version     int
}

type AggregateEvent struct {
	EventID       uuid.UUID
	Namespace     string
	AggregateID   uuid.UUID
	AggregateType eh.AggregateType
	EventType     eh.EventType
	RawEventData  json.RawMessage
	Timestamp     time.Time
	Version       int
	MetaData      map[string]interface{}
	data          eh.EventData
	RawMetaData   json.RawMessage
}

func (a AggregateEvent) MarshalBinary() (data []byte, err error) {
	return json.Marshal(a)
}

func (a *AggregateEvent) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, a)
}

// NewUUID for mocking in tests
var NewUUID = uuid.New

// newDBEvent returns a new dbEvent for an event.
func (s *EventStore) newDBEvent(ctx context.Context, event eh.Event) (*AggregateEvent, error) {
	ns := namespace.FromContext(ctx)

	// Marshal event data if there is any.
	rawEventData, err := s.encoder.Marshal(event.Data())
	if err != nil {
		return nil, eh.EventStoreError{
			BaseErr: err,
			Err:     ErrCouldNotMarshalEvent,
		}
	}

	// Marshal meta data if there is any.
	rawMetaData, err := json.Marshal(event.Metadata())
	if err != nil {
		return nil, eh.EventStoreError{
			BaseErr: err,
			Err:     ErrCouldNotMarshalEvent,
		}
	}

	return &AggregateEvent{
		EventID:       NewUUID(),
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Namespace:     ns,
		RawEventData:  rawEventData,
		RawMetaData:   rawMetaData,
	}, nil
}

// NewEventStore creates a new EventStore.
func NewEventStore(db redis.UniversalClient) (*EventStore, error) {

	if response := db.Ping(); response.Err() != nil {
		return nil, response.Err()
	}

	s := &EventStore{
		db:      db,
		encoder: &jsonEncoder{},
	}

	return s, nil
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	ns := namespace.FromContext(ctx)

	if len(events) == 0 {
		return eh.EventStoreError{
			Err: eh.ErrNoEventsToAppend,
		}
	}

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	dbEvents := make(map[string]interface{})
	aggregateID := events[0].AggregateID()
	version := originalVersion
	for _, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != aggregateID {
			return eh.EventStoreError{
				Err: eh.ErrInvalidEvent,
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != version+1 {
			return eh.EventStoreError{
				Err: eh.ErrIncorrectEventVersion,
			}
		}

		// Create the event record for the DB.
		e, err := s.newDBEvent(ctx, event)
		if err != nil {
			return err
		}
		dbEvents[strconv.Itoa(event.Version())] = *e
		version++
	}

	err := s.db.Watch(func(tx *redis.Tx) error {
		for version, event := range dbEvents {
			if result := tx.HSetNX(fmt.Sprintf("%s:%s", ns, aggregateID), version, event); result.Val() == false {
				return eh.EventStoreError{
					BaseErr: result.Err(),
					Err:     ErrVersionConflict,
				}
			}
		}
		return nil
	}, fmt.Sprintf("%s:%s", ns, aggregateID))

	if err != nil {
		return eh.EventStoreError{
			BaseErr: err,
			Err:     ErrCouldNotSaveAggregate,
		}
	}

	return nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (s *EventStore) Load(ctx context.Context, id uuid.UUID) ([]eh.Event, error) {
	ns := namespace.FromContext(ctx)
	cmd := s.db.HGetAll(fmt.Sprintf("%s:%s", ns, id.String()))
	var events []eh.Event

	for _, dbEvent := range cmd.Val() {
		e := AggregateEvent{}

		if err := json.Unmarshal([]byte(dbEvent), &e); err != nil {
			return nil, eh.EventStoreError{
				BaseErr: err,
				Err:     ErrCouldNotUnmarshalEvent,
			}
		}

		if e.RawEventData != nil {
			if eventData, err := s.encoder.Unmarshal(e.EventType, e.RawEventData); err != nil {
				return nil, eh.EventStoreError{
					BaseErr: err,
					Err:     ErrCouldNotUnmarshalEvent,
				}
			} else {
				e.data = eventData
			}
		}
		e.RawEventData = nil

		if e.RawMetaData != nil {
			if err := json.Unmarshal(e.RawMetaData, &e.MetaData); err != nil {
				return nil, eh.EventStoreError{
					BaseErr: err,
					Err:     ErrCouldNotUnmarshalEvent,
				}
			}
		}
		e.RawEventData = nil

		events = append(events, event{
			AggregateEvent: e,
		})
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].Version() < events[j].Version()
	})
	return events, nil
}

func (s *EventStore) Close() error {
	return s.db.Close()
}

// Clear clears the event storage.
func (s *EventStore) Clear(ctx context.Context) error {
	ns := namespace.FromContext(ctx)

	err := s.db.Watch(func(tx *redis.Tx) error {
		iter := tx.Scan(0, fmt.Sprintf("%s:*", ns), 0).Iterator()

		for iter.Next() {
			err := s.db.Del(iter.Val()).Err()
			if err != nil {
				return err
			}
		}
		if err := iter.Err(); err != nil {
			return err
		}

		return nil
	}, fmt.Sprintf("%s:*", ns))

	if err != nil {
		return eh.EventStoreError{
			BaseErr: err,
			Err:     ErrCouldNotClearDB,
		}
	}

	return nil
}

// event is the private implementation of the eventhorizon.Event interface
// for a redis event store.
type event struct {
	AggregateEvent
}

func (e event) Metadata() map[string]interface{} {
	return e.AggregateEvent.MetaData
}

// AggregateID implements the AggregateID method of the eventhorizon.Event interface.
func (e event) AggregateID() uuid.UUID {
	return e.AggregateEvent.AggregateID
}

// AggregateType implements the AggregateType method of the eventhorizon.Event interface.
func (e event) AggregateType() eh.AggregateType {
	return e.AggregateEvent.AggregateType
}

// EventType implements the EventType method of the eventhorizon.Event interface.
func (e event) EventType() eh.EventType {
	return e.AggregateEvent.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.AggregateEvent.data
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.AggregateEvent.Version
}

// Timestamp implements the Timestamp method of the eventhorizon.Event interface.
func (e event) Timestamp() time.Time {
	return e.AggregateEvent.Timestamp
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.AggregateEvent.EventType, e.AggregateEvent.Version)
}
