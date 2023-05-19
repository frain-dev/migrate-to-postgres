package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateSubscriptionsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting subscriptions collection migration")
	defer fmt.Println("Finished subscriptions collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.SubscriptionCollection)

	// pg := &PG{db: dbx}

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count subscriptions: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var subscriptions []datastore082.Subscription

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &subscriptions)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load subscriptions: %v", err)
		}

		if len(subscriptions) == 0 {
			break
		}
		lastID = subscriptions[len(subscriptions)-1].ID
		postgresSubscriptions := make([]*datastore09.Subscription, 0, len(subscriptions))

		for i := range subscriptions {
			s := &subscriptions[i]

			if !seen[s.UID] {
				seen[s.UID] = true
			} else {
				log.Errorf("subscription %s returned multiple times", s.UID)
				continue
			}

			projectID, ok := oldIDToNewID[s.ProjectID]
			if !ok {
				log.Errorf("new project id for project %s not found for subscription %s", s.ProjectID, s.UID)
				continue
			}

			endpointID, ok := oldIDToNewID[s.EndpointID]
			if !ok {
				log.Errorf("new endpoint id for endpoint %s not found for subscription %s", s.EndpointID, s.UID)
				continue
			}

			var deviceID string
			if !util.IsStringEmpty(s.DeviceID) {
				deviceID, ok = oldIDToNewID[s.DeviceID]
				if !ok {
					log.Errorf("new device id for device %s not found for subscription %s", s.DeviceID, s.UID)
					continue
				}
			}

			var sourceID string
			if !util.IsStringEmpty(s.SourceID) {
				sourceID, ok = oldIDToNewID[s.SourceID]
				if !ok {
					log.Errorf("new source id for source %s not found for subscription %s", s.SourceID, s.UID)
					continue
				}
			}

			postgresSubscription := datastore09.Subscription{
				UID:        s.UID,
				Name:       s.Name,
				Type:       datastore09.SubscriptionType(s.Type),
				ProjectID:  projectID,
				SourceID:   sourceID,
				EndpointID: endpointID,
				DeviceID:   deviceID,
				FilterConfig: &datastore09.FilterConfiguration{
					EventTypes: []string{"*"},
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
					Count:     s.AlertConfig.Count,
					Threshold: s.AlertConfig.Threshold,
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
				if len(s.FilterConfig.EventTypes) > 0 {
					postgresSubscription.FilterConfig.EventTypes = s.FilterConfig.EventTypes
				}

				if s.FilterConfig.Filter.Headers != nil {
					postgresSubscription.FilterConfig.Filter.Headers = s.FilterConfig.Filter.Headers
				}
				if s.FilterConfig.Filter.Body != nil {
					postgresSubscription.FilterConfig.Filter.Body = s.FilterConfig.Filter.Body
				}

			}

			if s.RateLimitConfig != nil {
				postgresSubscription.RateLimitConfig = &datastore09.RateLimitConfiguration{
					Count:    s.RateLimitConfig.Count,
					Duration: s.RateLimitConfig.Duration,
				}
			}

			oldIDToNewID[s.UID] = postgresSubscription.UID
			postgresSubscriptions = append(postgresSubscriptions, &postgresSubscription)
		}

		if len(postgresSubscriptions) > 0 {
			//err = pg.SaveSubscriptions(ctx, postgresSubscriptions)
			//if err != nil {
			//	return fmt.Errorf("failed to save postgres subscriptions: %v", err)
			//}
		}
	}

	return nil
}

const (
	createSubscription = `
    INSERT INTO convoy.subscriptions (
    id,name,type,
	project_id,endpoint_id,device_id,
	source_id,alert_config_count,alert_config_threshold,
	retry_config_type,retry_config_duration,
	retry_config_retry_count,filter_config_event_types,
	filter_config_filter_headers,filter_config_filter_body,
	rate_limit_config_count,rate_limit_config_duration,
	created_at, updated_at, deleted_at
	)
    VALUES ( 
        :id, :name, :type,
		:project_id, :endpoint_id, :device_id,
		:source_id, :alert_config_count, :alert_config_threshold,
		:retry_config_type, :retry_config_duration,
		:retry_config_retry_count, :filter_config_event_types,
		:filter_config_filter_headers, :filter_config_filter_body,
		:rate_limit_config_count, :rate_limit_config_duration,
		:created_at, :updated_at, :deleted_at
	);
    `
)

func (s *PG) SaveSubscriptions(ctx context.Context, subscriptions []*datastore09.Subscription) error {
	values := make([]map[string]interface{}, 0, len(subscriptions))
	for _, subscription := range subscriptions {

		ac := subscription.GetAlertConfig()
		rc := subscription.GetRetryConfig()
		fc := subscription.GetFilterConfig()
		rlc := subscription.GetRateLimitConfig()

		var endpointID, sourceID, deviceID *string
		if !util.IsStringEmpty(subscription.EndpointID) {
			endpointID = &subscription.EndpointID
		}

		if !util.IsStringEmpty(subscription.SourceID) {
			sourceID = &subscription.SourceID
		}

		if !util.IsStringEmpty(subscription.DeviceID) {
			deviceID = &subscription.DeviceID
		}

		values = append(values, map[string]interface{}{
			"id":                           subscription.UID,
			"name":                         subscription.Name,
			"type":                         subscription.Type,
			"project_id":                   subscription.ProjectID,
			"endpoint_id":                  endpointID,
			"device_id":                    deviceID,
			"source_id":                    sourceID,
			"alert_config_count":           ac.Count,
			"alert_config_threshold":       ac.Threshold,
			"retry_config_type":            rc.Type,
			"retry_config_duration":        rc.Duration,
			"retry_config_retry_count":     rc.RetryCount,
			"filter_config_event_types":    fc.EventTypes,
			"filter_config_filter_headers": fc.Filter.Headers,
			"filter_config_filter_body":    fc.Filter.Body,
			"rate_limit_config_count":      rlc.Count,
			"rate_limit_config_duration":   rlc.Duration,
			"created_at":                   subscription.CreatedAt,
			"updated_at":                   subscription.UpdatedAt,
			"deleted_at":                   subscription.DeletedAt,
		})
	}

	_, err := s.db.NamedExecContext(ctx, createSubscription, values)
	return err
}
