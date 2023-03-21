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

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateEventDeliveriesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting event delivery collection migration")
	defer fmt.Println("Finished event delivery collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.EventDeliveryCollection)

	const batchSize int64 = 3854 // very precise, see https://github.com/jmoiron/sqlx/issues/552#issuecomment-665630408

	pg := (&PG{db: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count event deliveries: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))

	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var eventDeliveries []datastore082.EventDelivery

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &eventDeliveries)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load eventDeliveries: %v", err)
		}
		fmt.Printf("batch %d loaded\n", i)

		if len(eventDeliveries) == 0 {
			break
		}

		lastID = eventDeliveries[len(eventDeliveries)-1].ID
		postgresEventDeliveries := make([]*datastore09.EventDelivery, 0, len(eventDeliveries))

		for i := range eventDeliveries {
			ed := &eventDeliveries[i]

			if !seen[ed.UID] {
				seen[ed.UID] = true
			} else {
				log.Errorf("event delivery %s returned multiple times", ed.UID)
				continue
			}

			projectID, ok := oldIDToNewID[ed.ProjectID]
			if !ok {
				log.Errorf("new project id for project %s not found for event delivery %s", ed.ProjectID, ed.UID)
				continue
			}

			endpointID, ok := oldIDToNewID[ed.EndpointID]
			if !ok {
				log.Errorf("new endpoint id for endpoint %s not found for event delivery %s", ed.EndpointID, ed.UID)
				continue
			}

			eventID, ok := oldIDToNewID[ed.EventID]
			if !ok {
				log.Errorf("new event id for event %s not found for event delivery %s", ed.EventID, ed.UID)
				continue
			}

			var deviceID string
			if !util.IsStringEmpty(ed.DeviceID) {
				deviceID, ok = oldIDToNewID[ed.DeviceID]
				if !ok {
					log.Errorf("new device id for device %s not found for event delivery %s", ed.DeviceID, ed.UID)
					continue
				}
			}

			subscriptionID, ok := oldIDToNewID[ed.SubscriptionID]
			if !ok {
				log.Errorf("new subscription id for subscription %s not found for event delivery %s", ed.SubscriptionID, ed.UID)
				continue
			}

			var sourceID string
			if ed.CLIMetadata != nil && !util.IsStringEmpty(ed.CLIMetadata.SourceID) {
				sourceID, ok = oldIDToNewID[ed.CLIMetadata.SourceID]
				if !ok {
					log.Errorf("new source id for source %s not found for event delivery %s", ed.CLIMetadata.SourceID, ed.UID)
					continue
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

			oldIDToNewID[ed.UID] = postgresEventDelivery.UID
			postgresEventDeliveries = append(postgresEventDeliveries, postgresEventDelivery)
		}

		if len(postgresEventDeliveries) > 0 {
			err = pg.SaveEventDeliveries(ctx, postgresEventDeliveries)
			if err != nil {
				return fmt.Errorf("failed to save postgres event deliveries: %v", err)
			}
		}

		fmt.Printf("Finished %d eventDeliveries batch\n", i)
	}

	return nil
}

const (
	saveEventDeliveries = `
    INSERT INTO convoy.event_deliveries (id,project_id,event_id,endpoint_id,device_id,subscription_id,headers,attempts,status,metadata,cli_metadata,description,created_at,updated_at,deleted_at)
    VALUES (
        :id, :project_id, :event_id, :endpoint_id, :device_id,
        :subscription_id, :headers, :attempts, :status, :metadata,
        :cli_metadata, :description, :created_at, :updated_at, :deleted_at
    )
    `
)

func (e *PG) SaveEventDeliveries(ctx context.Context, deliveries []*datastore09.EventDelivery) error {
	values := make([]map[string]interface{}, 0, len(deliveries))

	for _, delivery := range deliveries {
		var endpointID *string
		var deviceID *string

		if !util.IsStringEmpty(delivery.EndpointID) {
			endpointID = &delivery.EndpointID
		}

		if !util.IsStringEmpty(delivery.DeviceID) {
			deviceID = &delivery.DeviceID
		}

		fmt.Println("raw", delivery.Metadata.Raw)
		fmt.Println("data", delivery.Metadata.Data)

		values = append(values, map[string]interface{}{
			"id":              delivery.UID,
			"project_id":      delivery.ProjectID,
			"event_id":        delivery.EventID,
			"endpoint_id":     endpointID,
			"device_id":       deviceID,
			"subscription_id": delivery.SubscriptionID,
			"headers":         delivery.Headers,
			"attempts":        delivery.DeliveryAttempts,
			"status":          delivery.Status,
			"metadata":        delivery.Metadata,
			"cli_metadata":    delivery.CLIMetadata,
			"description":     delivery.Description,
			"created_at":      delivery.CreatedAt,
			"updated_at":      delivery.UpdatedAt,
			"deleted_at":      delivery.DeletedAt,
		})
	}

	_, err := e.db.NamedExecContext(ctx, saveEventDeliveries, values)
	return err
}
