package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/convoy/database/postgres"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jmoiron/sqlx"

	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

func migrateOrganisationsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting organisations collection migration")
	defer fmt.Println("Finished organisations collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.OrganisationCollection)

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count organisations: %v", err)
	}

	pgOrgRepo := postgres.NewOrgRepo(&PG{dbx: dbx})

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))

	for i := 1; i <= numBatches; i++ {
		var organisations []datastore082.Organisation

		_, err = store.FindMany(ctx, bson.M{}, nil, nil, int64(i), batchSize, &organisations)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load organisations: %v", err)
		}

		if len(organisations) == 0 {
			break
		}

		for i := range organisations {
			org := &organisations[i]

			ownerID, ok := oldIDToNewID[org.OwnerID]
			if !ok {
				log.Errorf("new owner id for owner %s not found for organisation %s", org.OwnerID, org.UID)
				continue
			}

			postgresOrg := &datastore09.Organisation{
				UID:            ulid.Make().String(),
				OwnerID:        ownerID,
				Name:           org.Name,
				CustomDomain:   null.NewString(org.CustomDomain, true),
				AssignedDomain: null.NewString(org.AssignedDomain, true),
				CreatedAt:      org.CreatedAt.Time(),
				UpdatedAt:      org.UpdatedAt.Time(),
				DeletedAt:      getDeletedAt(org.DeletedAt),
			}

			err = pgOrgRepo.CreateOrganisation(ctx, postgresOrg)
			if err != nil {
				return fmt.Errorf("failed to save postgres org: %v", err)
			}

			oldIDToNewID[org.UID] = postgresOrg.UID
		}
	}

	return nil
}
