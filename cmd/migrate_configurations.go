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

func migrateConfigurationsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.ConfigCollection)

	pgConfigRepo := postgres.NewConfigRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count configurations: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))

	for i := 1; i <= numBatches; i++ {
		var configurations []datastore082.Configuration

		_, err = store.FindMany(ctx, bson.M{}, nil, nil, int64(i), batchSize, &configurations)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load configurations: %v", err)
		}

		if len(configurations) == 0 {
			break
		}

		for i := range configurations {
			cfg := &configurations[i]

			postgresCfg := &datastore09.Configuration{
				UID:                ulid.Make().String(),
				IsAnalyticsEnabled: cfg.IsAnalyticsEnabled,
				IsSignupEnabled:    cfg.IsSignupEnabled,
				StoragePolicy:      nil,
				CreatedAt:          cfg.CreatedAt.Time(),
				UpdatedAt:          cfg.UpdatedAt.Time(),
				DeletedAt:          getDeletedAt(cfg.DeletedAt),
			}

			if cfg.StoragePolicy != nil {
				postgresCfg.StoragePolicy = &datastore09.StoragePolicyConfiguration{
					Type: datastore09.StorageType(cfg.StoragePolicy.Type),
				}

				if cfg.StoragePolicy.S3 != nil {
					postgresCfg.StoragePolicy.S3 = &datastore09.S3Storage{
						Bucket:       null.StringFrom(cfg.StoragePolicy.S3.Bucket),
						AccessKey:    null.StringFrom(cfg.StoragePolicy.S3.AccessKey),
						SecretKey:    null.StringFrom(cfg.StoragePolicy.S3.SecretKey),
						Region:       null.StringFrom(cfg.StoragePolicy.S3.Region),
						SessionToken: null.StringFrom(cfg.StoragePolicy.S3.SessionToken),
						Endpoint:     null.StringFrom(cfg.StoragePolicy.S3.Endpoint),
					}
				}

				if cfg.StoragePolicy.OnPrem != nil {
					postgresCfg.StoragePolicy.OnPrem = &datastore09.OnPremStorage{
						Path: null.StringFrom(cfg.StoragePolicy.OnPrem.Path),
					}
				}
			}

			err = pgConfigRepo.CreateConfiguration(ctx, postgresCfg)
			if err != nil {
				return fmt.Errorf("failed to save postgres cfg: %v", err)
			}

			oldIDToNewID[cfg.UID] = postgresCfg.UID

		}

	}

	return nil
}
