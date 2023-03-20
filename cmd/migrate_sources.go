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

func migrateSourcesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting sources collection migration")
	defer fmt.Println("Finished sources collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.SourceCollection)

	pgSourceRepo := postgres.NewSourceRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count sources: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var sources []datastore082.Source

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &sources)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load sources: %v", err)
		}

		if len(sources) == 0 {
			break
		}
		lastID = sources[len(sources)-1].ID

		for i := range sources {
			source := &sources[i]

			if !seen[source.UID] {
				seen[source.UID] = true
			} else {
				log.Errorf("source %s returned multiple times", source.UID)
				continue
			}

			projectID, ok := oldIDToNewID[source.ProjectID]
			if !ok {
				log.Errorf("new project id for project %s not found for source %s", source.ProjectID, source.UID)
				continue
			}

			postgresSource := &datastore09.Source{
				UID:            ulid.Make().String(),
				ProjectID:      projectID,
				MaskID:         source.MaskID,
				Name:           source.Name,
				Type:           datastore09.SourceType(source.Type),
				Provider:       datastore09.SourceProvider(source.Provider),
				IsDisabled:     source.IsDisabled,
				ForwardHeaders: source.ForwardHeaders,
				PubSub:         nil,
				CreatedAt:      source.CreatedAt.Time(),
				UpdatedAt:      source.UpdatedAt.Time(),
				DeletedAt:      getDeletedAt(source.DeletedAt),
			}

			if source.ProviderConfig != nil {
				postgresSource.ProviderConfig = &datastore09.ProviderConfig{}
				if source.ProviderConfig.Twitter != nil {
					postgresSource.ProviderConfig.Twitter.CrcVerifiedAt = null.TimeFrom(source.ProviderConfig.Twitter.CrcVerifiedAt.Time())
				}
			}

			if source.Verifier != nil {
				postgresSource.Verifier = &datastore09.VerifierConfig{
					Type: datastore09.VerifierType(source.Verifier.Type),
				}

				if source.Verifier.ApiKey != nil {
					postgresSource.Verifier.ApiKey = &datastore09.ApiKey{
						HeaderValue: source.Verifier.ApiKey.HeaderValue,
						HeaderName:  source.Verifier.ApiKey.HeaderName,
					}
				}

				if source.Verifier.HMac != nil {
					postgresSource.Verifier.HMac = &datastore09.HMac{
						Header:   source.Verifier.HMac.Header,
						Hash:     source.Verifier.HMac.Hash,
						Secret:   source.Verifier.HMac.Secret,
						Encoding: datastore09.EncodingType(source.Verifier.HMac.Encoding),
					}
				}

				if source.Verifier.BasicAuth != nil {
					postgresSource.Verifier.BasicAuth = &datastore09.BasicAuth{
						UserName: source.Verifier.BasicAuth.UserName,
						Password: source.Verifier.BasicAuth.Password,
					}
				}
			}

			err = pgSourceRepo.CreateSource(ctx, postgresSource)
			if err != nil {
				return fmt.Errorf("failed to save postgres source: %v", err)
			}

			oldIDToNewID[source.UID] = postgresSource.UID
		}
	}

	return nil
}
