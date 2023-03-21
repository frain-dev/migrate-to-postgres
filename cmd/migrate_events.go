package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	"github.com/frain-dev/convoy/pkg/httpheader"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateEventsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting events collection migration")
	defer fmt.Println("Finished events collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.EventCollection)

	pg := &PG{db: dbx}

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count events: %v", err)
	}

	var lastID primitive.ObjectID
	seen := map[string]bool{}
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))

	for i := 1; i <= numBatches; i++ {
		var events []datastore082.Event

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &events)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load events: %v", err)
		}
		fmt.Printf("batch %d loaded\n", i)
		if len(events) == 0 {
			break
		}

		lastID = events[len(events)-1].ID
		postgresEvents := make([]*datastore09.Event, 0, len(events))

		for i := range events {
			event := &events[i]

			if !seen[event.UID] {
				seen[event.UID] = true
			} else {
				log.Errorf("event %s returned multiple times", event.UID)
				continue
			}

			projectID, ok := oldIDToNewID[event.ProjectID]
			if !ok {
				log.Errorf("new project id for project %s not found for event %s", event.ProjectID, event.UID)
				continue
			}

			endpoints := make([]string, 0, len(event.Endpoints))

			for _, id := range event.Endpoints {
				if !util.IsStringEmpty(id) {
					newID, ok := oldIDToNewID[id]
					if !ok {
						log.Errorf("new endpoint id for endpoint %s not found for event %s", id, event.UID)
						continue
					}

					endpoints = append(endpoints, newID)
				}
			}

			var sourceID string
			if !util.IsStringEmpty(event.SourceID) {
				sourceID, ok = oldIDToNewID[event.SourceID]
				if !ok {
					log.Errorf("new source id for source %s not found for event %s", event.SourceID, event.UID)
					continue
				}
			}

			postgresEvent := &datastore09.Event{
				UID:              ulid.Make().String(),
				EventType:        datastore09.EventType(event.EventType),
				MatchedEndpoints: event.MatchedEndpoints,
				SourceID:         sourceID,
				AppID:            event.AppID,
				ProjectID:        projectID,
				Endpoints:        endpoints,
				Headers:          httpheader.HTTPHeader(event.Headers),
				Data:             event.Data,
				Raw:              event.Raw,
				CreatedAt:        event.CreatedAt.Time(),
				UpdatedAt:        event.UpdatedAt.Time(),
				DeletedAt:        getDeletedAt(event.DeletedAt),
			}

			oldIDToNewID[event.UID] = postgresEvent.UID
			postgresEvents = append(postgresEvents, postgresEvent)
		}

		if len(postgresEvents) > 0 {
			err = pg.SaveEvents(ctx, postgresEvents)
			if err != nil {
				return fmt.Errorf("failed to save postgres events: %v", err)
			}
		}
		fmt.Printf("Finished %d events batch\n", i)
	}

	return nil
}

const (
	saveEvents = `
	INSERT INTO convoy.events (id, event_type, endpoints, project_id, source_id, headers, raw, data,created_at,updated_at, deleted_at)
	VALUES (
	    :id, :event_type, :endpoints, :project_id, :source_id,
	    :headers, :raw, :data, :created_at, :updated_at, :deleted_at
	)
	`

	createEventEndpoints = `
	INSERT INTO convoy.events_endpoints (endpoint_id, event_id) VALUES (:endpoint_id, :event_id)
	`
)

func (e *PG) SaveEvents(ctx context.Context, events []*datastore09.Event) error {
	ev := make([]map[string]interface{}, 0, len(events))
	evEndpoints := make([]postgres.EventEndpoint, 0, len(events)*2)

	for _, event := range events {
		var sourceID *string

		if !util.IsStringEmpty(event.SourceID) {
			sourceID = &event.SourceID
		}

		ev = append(ev, map[string]interface{}{
			"id":         event.UID,
			"event_type": event.EventType,
			"endpoints":  event.Endpoints,
			"project_id": event.ProjectID,
			"source_id":  sourceID,
			"headers":    event.Headers,
			"raw":        event.Raw,
			"data":       event.Data,
			"created_at": event.CreatedAt,
			"updated_at": event.UpdatedAt,
			"deleted_at": event.DeletedAt,
		})

		if len(event.Endpoints) > 0 {
			for _, endpointID := range event.Endpoints {
				evEndpoints = append(evEndpoints, postgres.EventEndpoint{EventID: event.UID, EndpointID: endpointID})
			}
		}
	}

	tx, err := e.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	defer rollbackTx(tx)

	_, err = tx.NamedExecContext(ctx, saveEvents, ev)
	if err != nil {
		return err
	}

	_, err = tx.NamedExecContext(ctx, createEventEndpoints, evEndpoints)
	if err != nil {
		return err
	}

	return tx.Commit()
}
