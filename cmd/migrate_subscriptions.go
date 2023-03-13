package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateSubscriptionsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.SubscriptionCollection)

	pgSubscriptionRepo := postgres.NewSubscriptionRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count portal links: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 1; i <= numBatches; i++ {
		var subscriptions []datastore082.Subscription

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &subscriptions)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load subscriptions: %v", err)
		}

		if len(subscriptions) == 0 {
			break
		}

		for i := range subscriptions {
			s := &subscriptions[i]

			projectID, ok := oldIDToNewID[s.ProjectID]
			if !ok {
				return fmt.Errorf("new project id for project %s not found for subscription %s", s.ProjectID, s.UID)
			}

			endpointID, ok := oldIDToNewID[s.EndpointID]
			if !ok {
				return fmt.Errorf("new endpoint id for endpoint %s not found for subscription %s", s.EndpointID, s.UID)
			}

			var deviceID string
			if !util.IsStringEmpty(s.DeviceID) {
				deviceID, ok = oldIDToNewID[s.DeviceID]
				if !ok {
					return fmt.Errorf("new device id for device %s not found for subscription %s", s.DeviceID, s.UID)
				}
			}

			var sourceID string
			if !util.IsStringEmpty(s.SourceID) {
				sourceID, ok = oldIDToNewID[s.SourceID]
				if !ok {
					return fmt.Errorf("new source id for source %s not found for subscription %s", s.SourceID, s.UID)
				}
			}

			postgresSubscription := &datastore09.Subscription{
				UID:        ulid.Make().String(),
				Name:       s.Name,
				Type:       datastore09.SubscriptionType(s.Type),
				ProjectID:  projectID,
				SourceID:   sourceID,
				EndpointID: endpointID,
				DeviceID:   deviceID,
				FilterConfig: &datastore09.FilterConfiguration{
					EventTypes: nil,
					Filter: datastore09.FilterSchema{
						Headers: datastore09.M{},
						Body:    datastore09.M{},
					},
				},
				CreatedAt: s.CreatedAt.Time(),
				UpdatedAt: s.UpdatedAt.Time(),
				DeletedAt: getDeletedAt(s.DeletedAt),
			}

			if s.AlertConfig != nil {
				postgresSubscription.AlertConfig = &datastore09.AlertConfiguration{
					Count:     postgresSubscription.AlertConfig.Count,
					Threshold: postgresSubscription.AlertConfig.Threshold,
				}
			}

			if s.RetryConfig != nil {
				postgresSubscription.RetryConfig = &datastore09.RetryConfiguration{
					Type:       datastore09.StrategyProvider(s.RetryConfig.Type),
					Duration:   s.RetryConfig.Duration,
					RetryCount: s.RetryConfig.RetryCount,
				}
			}

			if s.FilterConfig != nil {
				postgresSubscription.FilterConfig = &datastore09.FilterConfiguration{
					EventTypes: s.FilterConfig.EventTypes,
					Filter: datastore09.FilterSchema{
						Headers: s.FilterConfig.Filter.Headers,
						Body:    s.FilterConfig.Filter.Body,
					},
				}

				if postgresSubscription.FilterConfig.Filter.Headers == nil {
					postgresSubscription.FilterConfig.Filter.Headers = datastore09.M{}
				}

				if postgresSubscription.FilterConfig.Filter.Body == nil {
					postgresSubscription.FilterConfig.Filter.Body = datastore09.M{}
				}
			}

			if s.RateLimitConfig != nil {
				postgresSubscription.RateLimitConfig = &datastore09.RateLimitConfiguration{
					Count:    s.RateLimitConfig.Count,
					Duration: s.RateLimitConfig.Duration,
				}
			}

			err = pgSubscriptionRepo.CreateSubscription(ctx, postgresSubscription.ProjectID, postgresSubscription)
			if err != nil {
				return fmt.Errorf("failed to save postgres subscription: %v", err)
			}

			oldIDToNewID[s.UID] = postgresSubscription.UID
		}

		pagination.Next = pager.Next
	}

	return nil
}
