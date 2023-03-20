package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

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
	fmt.Println("Starting endpoint collection migration")
	defer fmt.Println("Finished endpoint collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.EndpointCollection)

	pgProjectRepo := postgres.NewEndpointRepo(&PG{dbx: dbx})

	totalEndpoints, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count endpoints: %v", err)
	}

	numBatches := int(math.Ceil(float64(totalEndpoints) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var endpoints []datastore082.Endpoint

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &endpoints)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load endpoints: %v", err)
		}

		if len(endpoints) == 0 {
			break
		}
		lastID = endpoints[len(endpoints)-1].ID

		for i := range endpoints {
			endpoint := &endpoints[i]

			if !seen[endpoint.UID] {
				seen[endpoint.UID] = true
			} else {
				log.Errorf("endpoint %s returned multiple times", endpoint.UID)
				continue
			}

			projectID, ok := oldIDToNewID[endpoint.ProjectID]
			if !ok {
				log.Errorf("new project id for project %s not found for endpoint %s", endpoint.ProjectID, endpoint.UID)
				continue
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

			if postgresEndpoint.Secrets == nil {
				postgresEndpoint.Secrets = datastore09.Secrets{}
			}

			err = pgProjectRepo.CreateEndpoint(ctx, postgresEndpoint, postgresEndpoint.ProjectID)
			if err != nil {
				return fmt.Errorf("failed to save postgres endpoint: %v", err)
			}

			oldIDToNewID[endpoint.UID] = postgresEndpoint.UID
		}
	}

	return nil
}
