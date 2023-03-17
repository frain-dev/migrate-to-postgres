package main

import (
	"context"
	"errors"
	"fmt"
	"math"

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

func migrateEventDeliveriesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting event delivery collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.EventDeliveryCollection)

	pgEventDeliveryRepo := postgres.NewEventDeliveryRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count event deliveries: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))

	for i := 1; i <= numBatches; i++ {
		var eventDeliveries []datastore082.EventDelivery

		_, err = store.FindMany(ctx, bson.M{}, nil, nil, int64(i), batchSize, &eventDeliveries)
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

			projectID, ok := oldIDToNewID[ed.ProjectID]
			if !ok {
				return fmt.Errorf("new project id for project %s not found for event delivery %s", ed.ProjectID, ed.UID)
			}

			endpointID, ok := oldIDToNewID[ed.EndpointID]
			if !ok {
				return fmt.Errorf("new endpoint id for endpoint %s not found for event delivery %s", ed.EndpointID, ed.UID)
			}

			eventID, ok := oldIDToNewID[ed.EventID]
			if !ok {
				return fmt.Errorf("new event id for event %s not found for event delivery %s", ed.EventID, ed.UID)
			}

			var deviceID string
			if !util.IsStringEmpty(ed.DeviceID) {
				deviceID, ok = oldIDToNewID[ed.DeviceID]
				if !ok {
					return fmt.Errorf("new device id for device %s not found for event delivery %s", ed.DeviceID, ed.UID)
				}
			}

			subscriptionID, ok := oldIDToNewID[ed.SubscriptionID]
			if !ok {
				return fmt.Errorf("new subscription id for subscription %s not found for event delivery %s", ed.SubscriptionID, ed.UID)
			}

			var sourceID string
			if ed.CLIMetadata != nil && !util.IsStringEmpty(ed.CLIMetadata.SourceID) {
				sourceID, ok = oldIDToNewID[ed.CLIMetadata.SourceID]
				if !ok {
					return fmt.Errorf("new source id for source %s not found for event delivery %s", ed.CLIMetadata.SourceID, ed.UID)
				}
			}

			postgresEventDelivery := &datastore09.EventDelivery{
				UID:            ulid.Make().String(),
				ProjectID:      projectID,
				EventID:        eventID,
				EndpointID:     endpointID,
				DeviceID:       deviceID,
				SubscriptionID: subscriptionID,
				Headers:        httpheader.HTTPHeader(ed.Headers),
				Status:         datastore09.EventDeliveryStatus(ed.Status),
				Description:    ed.Description,
				CreatedAt:      ed.CreatedAt.Time(),
				UpdatedAt:      ed.UpdatedAt.Time(),
				DeletedAt:      getDeletedAt(ed.DeletedAt),
			}
			if ed.CLIMetadata != nil {
				postgresEventDelivery.CLIMetadata = &datastore09.CLIMetadata{
					EventType: ed.CLIMetadata.EventType,
					SourceID:  sourceID,
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
					EndpointID:       endpointID,
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
					DeletedAt:        getDeletedAt(attempt.DeletedAt),
				})
			}

			err = pgEventDeliveryRepo.CreateEventDelivery(ctx, postgresEventDelivery)
			if err != nil {
				return fmt.Errorf("failed to save postgres event delivery: %v", err)
			}

			oldIDToNewID[ed.UID] = postgresEventDelivery.UID
		}
	}

	fmt.Println("Finished event delivery collection migration")
	return nil
}
