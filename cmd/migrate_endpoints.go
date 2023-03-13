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

func migrateEndpointsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.EndpointCollection)

	pgProjectRepo := postgres.NewEndpointRepo(&PG{dbx: dbx})

	totalEndpoints, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count projects: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(totalEndpoints) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 1; i <= numBatches; i++ {
		var endpoints []datastore082.Endpoint

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &endpoints)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load endpoints: %v", err)
		}

		if len(endpoints) == 0 {
			break
		}

		for i := range endpoints {
			endpoint := &endpoints[i]

			projectID, ok := oldIDToNewID[endpoint.ProjectID]
			if !ok {
				return fmt.Errorf("new project id for project %s not found for endpoint %s", endpoint.ProjectID, endpoint.UID)
			}

			postgresEndpoint := &datastore09.Endpoint{
				UID:                ulid.Make().String(),
				ProjectID:          projectID,
				OwnerID:            endpoint.OwnerID,
				TargetURL:          endpoint.TargetURL,
				Title:              endpoint.Title,
				AdvancedSignatures: endpoint.AdvancedSignatures,
				Description:        endpoint.Description,
				SlackWebhookURL:    endpoint.SlackWebhookURL,
				SupportEmail:       endpoint.SupportEmail,
				AppID:              endpoint.AppID,
				HttpTimeout:        endpoint.HttpTimeout,
				RateLimit:          endpoint.RateLimit,
				Status:             datastore09.EndpointStatus(endpoint.Status),
				RateLimitDuration:  endpoint.RateLimitDuration,
				CreatedAt:          endpoint.CreatedAt.Time(),
				UpdatedAt:          endpoint.UpdatedAt.Time(),
				DeletedAt:          getDeletedAt(endpoint.DeletedAt),
			}

			if endpoint.Authentication != nil {
				postgresEndpoint.Authentication = &datastore09.EndpointAuthentication{
					Type: datastore09.EndpointAuthenticationType(endpoint.Authentication.Type),
				}

				if endpoint.Authentication.ApiKey != nil {
					postgresEndpoint.Authentication.ApiKey = &datastore09.ApiKey{
						HeaderValue: endpoint.Authentication.ApiKey.HeaderValue,
						HeaderName:  endpoint.Authentication.ApiKey.HeaderName,
					}
				}
			}

			for _, secret := range endpoint.Secrets {
				postgresEndpoint.Secrets = append(postgresEndpoint.Secrets, datastore09.Secret{
					UID:       ulid.Make().String(),
					Value:     secret.Value,
					CreatedAt: secret.CreatedAt.Time(),
					UpdatedAt: secret.UpdatedAt.Time(),
					ExpiresAt: null.NewTime(secret.ExpiresAt.Time(), true),
					DeletedAt: getDeletedAt(secret.DeletedAt),
				})
			}

			err = pgProjectRepo.CreateEndpoint(ctx, postgresEndpoint, postgresEndpoint.ProjectID)
			if err != nil {
				return fmt.Errorf("failed to save postgres endpoint: %v", err)
			}

			oldIDToNewID[endpoint.UID] = postgresEndpoint.UID
		}

		pagination.Next = pager.Next
	}

	return nil
}
