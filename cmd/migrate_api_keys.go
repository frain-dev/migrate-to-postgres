package main

import (
	"context"
	"errors"
	"fmt"
	"math"

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
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.APIKeyCollection)

	pgAPIKeyRepo := postgres.NewAPIKeyRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count portal links: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 0; i < numBatches; i++ {
		var apiKeys []datastore082.APIKey

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &apiKeys)
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

			postgresAPIKey := &datastore09.APIKey{
				UID:    ulid.Make().String(),
				MaskID: ak.MaskID,
				Name:   ak.Name,
				Role: auth09.Role{
					Type:     auth09.RoleType(ak.Role.Type),
					Project:  ak.Role.Project,
					Endpoint: ak.Role.Endpoint,
				},
				Hash:      ak.Hash,
				Salt:      ak.Salt,
				Type:      datastore09.KeyType(ak.Type),
				UserID:    ak.UserID,
				ExpiresAt: null.TimeFrom(ak.ExpiresAt.Time()),
				CreatedAt: ak.CreatedAt.Time(),
				UpdatedAt: ak.UpdatedAt.Time(),
				DeletedAt: null.TimeFrom(ak.DeletedAt.Time()),
			}

			err = pgAPIKeyRepo.CreateAPIKey(ctx, postgresAPIKey)
			if err != nil {
				return fmt.Errorf("failed to save postgres api key: %v", err)
			}
		}

		pagination.Next = pager.Next
	}

	return nil
}
