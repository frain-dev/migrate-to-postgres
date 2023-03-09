package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/convoy/pkg/httpheader"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

func migrateEventsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.EventCollection)

	pgEventRepo := postgres.NewEventRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count portal links: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 0; i < numBatches; i++ {
		var events []datastore082.Event

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &events)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load events: %v", err)
		}

		if len(events) == 0 {
			break
		}

		for i := range events {
			event := &events[i]

			postgresEvent := &datastore09.Event{
				UID:              ulid.Make().String(),
				EventType:        datastore09.EventType(event.EventType),
				MatchedEndpoints: event.MatchedEndpoints,
				SourceID:         event.SourceID,
				AppID:            event.AppID,
				ProjectID:        event.ProjectID,
				Endpoints:        event.Endpoints,
				Headers:          httpheader.HTTPHeader(event.Headers),
				Data:             event.Data,
				Raw:              event.Raw,
				CreatedAt:        event.CreatedAt.Time(),
				UpdatedAt:        event.UpdatedAt.Time(),
				DeletedAt:        null.TimeFrom(event.DeletedAt.Time()),
			}

			err = pgEventRepo.CreateEvent(ctx, postgresEvent)
			if err != nil {
				return fmt.Errorf("failed to save postgres event: %v", err)
			}
		}

		pagination.Next = pager.Next
	}

	return nil
}
