package main

import (
	"context"
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

	pgEventRepo := postgres.NewEventRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count events: %v", err)
	}

	var lastID primitive.ObjectID

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

		if len(events) == 0 {
			break
		}

		lastID = events[len(events)-1].ID

		for i := range events {
			event := &events[i]

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

			err = pgEventRepo.CreateEvent(ctx, postgresEvent)
			if err != nil {
				return fmt.Errorf("failed to save postgres event: %v", err)
			}

			oldIDToNewID[event.UID] = postgresEvent.UID
		}

		fmt.Printf("Finished %s events batch\n", i)
	}

	return nil
}
