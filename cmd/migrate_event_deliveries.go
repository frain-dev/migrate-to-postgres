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

func migrateEventDeliveriesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.EventDeliveryCollection)

	pgEventDeliveryRepo := postgres.NewEventDeliveryRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count portal links: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 0; i < numBatches; i++ {
		var eventDeliveries []datastore082.EventDelivery

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &eventDeliveries)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load eventDeliveries: %v", err)
		}

		if len(eventDeliveries) == 0 {
			break
		}

		for i := range eventDeliveries {
			ed := &eventDeliveries[i]

			postgresEventDelivery := &datastore09.EventDelivery{
				UID:            ulid.Make().String(),
				ProjectID:      ed.ProjectID,
				EventID:        ed.EventID,
				EndpointID:     ed.EndpointID,
				DeviceID:       ed.DeviceID,
				SubscriptionID: ed.SubscriptionID,
				Headers:        httpheader.HTTPHeader(ed.Headers),
				Status:         datastore09.EventDeliveryStatus(ed.Status),
				Description:    ed.Description,
				CreatedAt:      ed.CreatedAt.Time(),
				UpdatedAt:      ed.UpdatedAt.Time(),
				DeletedAt:      null.TimeFrom(ed.DeletedAt.Time()),
			}
			if ed.CLIMetadata != nil {
				postgresEventDelivery.CLIMetadata = &datastore09.CLIMetadata{
					EventType: ed.CLIMetadata.EventType,
					SourceID:  ed.CLIMetadata.SourceID,
				}
			}

			if ed.Metadata != nil {
				postgresEventDelivery.Metadata = &datastore09.Metadata{
					Data:            ed.Metadata.Data,
					Raw:             ed.Metadata.Raw,
					Strategy:        datastore09.StrategyProvider(ed.Metadata.Strategy),
					NextSendTime:    ed.Metadata.NextSendTime.Time(),
					NumTrials:       ed.Metadata.NumTrials,
					IntervalSeconds: ed.Metadata.IntervalSeconds,
					RetryLimit:      ed.Metadata.RetryLimit,
				}
			}

			for _, attempt := range ed.DeliveryAttempts {
				postgresEventDelivery.DeliveryAttempts = append(postgresEventDelivery.DeliveryAttempts, datastore09.DeliveryAttempt{
					UID:              ulid.Make().String(),
					MsgID:            attempt.MsgID,
					URL:              attempt.URL,
					Method:           attempt.Method,
					EndpointID:       attempt.EndpointID,
					APIVersion:       attempt.APIVersion,
					IPAddress:        attempt.IPAddress,
					RequestHeader:    datastore09.HttpHeader(attempt.RequestHeader),
					ResponseHeader:   datastore09.HttpHeader(attempt.ResponseHeader),
					HttpResponseCode: attempt.HttpResponseCode,
					ResponseData:     attempt.ResponseData,
					Error:            attempt.Error,
					Status:           attempt.Status,
					CreatedAt:        attempt.CreatedAt.Time(),
					UpdatedAt:        attempt.UpdatedAt.Time(),
					DeletedAt:        null.TimeFrom(attempt.DeletedAt.Time()),
				})
			}

			err = pgEventDeliveryRepo.CreateEventDelivery(ctx, postgresEventDelivery)
			if err != nil {
				return fmt.Errorf("failed to save postgres event delivery: %v", err)
			}
		}

		pagination.Next = pager.Next
	}

	return nil
}
