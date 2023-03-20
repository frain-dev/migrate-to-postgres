package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	auth09 "github.com/frain-dev/convoy/auth"
	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

func migrateAPIKeysCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting api key collection migration")
	defer fmt.Println("Finished api key collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.APIKeyCollection)

	pgAPIKeyRepo := postgres.NewAPIKeyRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count api keys: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))

	for i := 1; i <= numBatches; i++ {
		var apiKeys []datastore082.APIKey

		_, err = store.FindMany(ctx, bson.M{}, nil, nil, int64(i), batchSize, &apiKeys)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load apiKeys: %v", err)
		}

		if len(apiKeys) == 0 {
			break
		}

		for i := range apiKeys {
			ak := &apiKeys[i]
			var ok bool

			var projectID string
			if !util.IsStringEmpty(ak.Role.Project) {
				projectID, ok = oldIDToNewID[ak.Role.Project]
				if !ok {
					log.Errorf("new project id for project %s not found for api key %s", ak.Role.Project, ak.UID)
					continue
				}
			}

			var endpointID string
			if !util.IsStringEmpty(ak.Role.Endpoint) {
				endpointID, ok = oldIDToNewID[ak.Role.Endpoint]
				if !ok {
					log.Errorf("new endpoint id for endpoint %s not found for api key %s", ak.Role.Endpoint, ak.UID)
					continue
				}
			}

			var userID string
			if !util.IsStringEmpty(ak.UserID) {
				userID, ok = oldIDToNewID[ak.UserID]
				if !ok {
					log.Errorf("new user id for user %s not found for api key %s", ak.UserID, ak.UID)
					continue
				}
			}

			postgresAPIKey := &datastore09.APIKey{
				UID:    ulid.Make().String(),
				MaskID: ak.MaskID,
				Name:   ak.Name,
				Role: auth09.Role{
					Type:     auth09.RoleType(ak.Role.Type),
					Project:  projectID,
					Endpoint: endpointID,
				},
				Hash:      ak.Hash,
				Salt:      ak.Salt,
				Type:      datastore09.KeyType(ak.Type),
				UserID:    userID,
				ExpiresAt: null.TimeFrom(ak.ExpiresAt.Time()),
				CreatedAt: ak.CreatedAt.Time(),
				UpdatedAt: ak.UpdatedAt.Time(),
				DeletedAt: getDeletedAt(ak.DeletedAt),
			}

			err = pgAPIKeyRepo.CreateAPIKey(ctx, postgresAPIKey)
			if err != nil {
				return fmt.Errorf("failed to save postgres api key: %v", err)
			}

			oldIDToNewID[ak.UID] = postgresAPIKey.UID
		}
	}

	return nil
}
