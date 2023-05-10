package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateConfigurationsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting configuration collection migration")
	defer fmt.Println("Finished configuration collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.ConfigCollection)

	pgConfigRepo := postgres.NewConfigRepo(&PG{db: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count configurations: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var configurations []datastore082.Configuration

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &configurations)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load configurations: %v", err)
		}

		if len(configurations) == 0 {
			break
		}
		lastID = configurations[len(configurations)-1].ID

		for i := range configurations {
			cfg := &configurations[i]

			if !seen[cfg.UID] {
				seen[cfg.UID] = true
			} else {
				log.Errorf("config %s returned multiple times", cfg.UID)
				continue
			}

			postgresCfg := &datastore09.Configuration{
				UID:                cfg.UID,
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
						Bucket:       nullString(cfg.StoragePolicy.S3.Bucket),
						AccessKey:    nullString(cfg.StoragePolicy.S3.AccessKey),
						SecretKey:    nullString(cfg.StoragePolicy.S3.SecretKey),
						Region:       nullString(cfg.StoragePolicy.S3.Region),
						SessionToken: nullString(cfg.StoragePolicy.S3.SessionToken),
						Endpoint:     nullString(cfg.StoragePolicy.S3.Endpoint),
					}
				}

				if cfg.StoragePolicy.OnPrem != nil {
					postgresCfg.StoragePolicy.OnPrem = &datastore09.OnPremStorage{
						Path: nullString(cfg.StoragePolicy.OnPrem.Path),
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
