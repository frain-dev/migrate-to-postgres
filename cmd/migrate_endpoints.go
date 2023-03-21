package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

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

	pg := &PG{db: dbx}

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

		postgresEndpoints := make([]*datastore09.Endpoint, 0, len(endpoints))

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

			postgresEndpoint := datastore09.Endpoint{
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

			oldIDToNewID[endpoint.UID] = postgresEndpoint.UID
			postgresEndpoints = append(postgresEndpoints, &postgresEndpoint)
		}

		if len(postgresEndpoints) > 0 {
			err = pg.SaveEndpoints(ctx, postgresEndpoints)
			if err != nil {
				return fmt.Errorf("failed to save postgres endpoints: %v", err)
			}
		}
	}

	return nil
}

const (
	saveEndpoints = `
	INSERT INTO convoy.endpoints (
		id, title, status, secrets, owner_id, target_url, description, http_timeout,
		rate_limit, rate_limit_duration, advanced_signatures, slack_webhook_url,
		support_email, app_id, project_id, authentication_type, authentication_type_api_key_header_name,
		authentication_type_api_key_header_value, created_at, updated_at, deleted_at
	)
	VALUES
	  (
		:id, :title, :status, :secrets, :owner_id, :target_url, :description, :http_timeout,
		:rate_limit, :rate_limit_duration, :advanced_signatures, :slack_webhook_url,
		:support_email, :app_id, :project_id, :authentication_type, :authentication_type_api_key_header_name,
		:authentication_type_api_key_header_value, :created_at, :updated_at, :deleted_at
	  )
	`
)

func (e *PG) SaveEndpoints(ctx context.Context, endpoints []*datastore09.Endpoint) error {
	values := make([]map[string]interface{}, 0, len(endpoints))

	for _, endpoint := range endpoints {
		ac := endpoint.GetAuthConfig()

		values = append(values, map[string]interface{}{
			"id":                  endpoint.UID,
			"title":               endpoint.Title,
			"status":              endpoint.Status,
			"secrets":             endpoint.Secrets,
			"owner_id":            endpoint.OwnerID,
			"target_url":          endpoint.TargetURL,
			"description":         endpoint.Description,
			"http_timeout":        endpoint.HttpTimeout,
			"rate_limit":          endpoint.RateLimit,
			"rate_limit_duration": endpoint.RateLimitDuration,
			"advanced_signatures": endpoint.AdvancedSignatures,
			"slack_webhook_url":   endpoint.SlackWebhookURL,
			"support_email":       endpoint.SupportEmail,
			"app_id":              endpoint.AppID,
			"project_id":          endpoint.ProjectID,
			"authentication_type": ac.Type,
			"authentication_type_api_key_header_name":  ac.ApiKey.HeaderName,
			"authentication_type_api_key_header_value": ac.ApiKey.HeaderValue,
			"created_at": endpoint.CreatedAt,
			"updated_at": endpoint.UpdatedAt,
			"deleted_at": endpoint.DeletedAt,
		})
	}

	_, err := e.db.NamedExecContext(ctx, saveEndpoints, values)
	return err
}

func rollbackTx(tx *sqlx.Tx) {
	err := tx.Rollback()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		log.WithError(err).Error("failed to rollback tx")
	}
}
