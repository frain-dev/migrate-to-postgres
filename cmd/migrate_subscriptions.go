package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
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

	for i := 0; i < numBatches; i++ {
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

			postgresSubscription := &datastore09.Subscription{
				UID:        ulid.Make().String(),
				Name:       s.Name,
				Type:       datastore09.SubscriptionType(s.Type),
				ProjectID:  s.ProjectID,
				SourceID:   s.SourceID,
				EndpointID: s.EndpointID,
				DeviceID:   s.DeviceID,
				CreatedAt:  s.CreatedAt.Time(),
				UpdatedAt:  s.UpdatedAt.Time(),
				DeletedAt:  null.NewTime(s.DeletedAt.Time(), true),
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
		}

		pagination.Next = pager.Next
	}

	return nil
}
