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

func migratePortalLinksCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.PortalLinkCollection)

	pgPortalLinkRepo := postgres.NewPortalLinkRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count portal links: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 0; i < numBatches; i++ {
		var portalLinks []datastore082.PortalLink

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &portalLinks)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load portalLinks: %v", err)
		}

		if len(portalLinks) == 0 {
			break
		}

		for i := range portalLinks {
			portalLink := &portalLinks[i]

			postgresPortalLink := &datastore09.PortalLink{
				UID:       ulid.Make().String(),
				Name:      portalLink.Name,
				ProjectID: portalLink.ProjectID,
				Token:     portalLink.Token,
				Endpoints: portalLink.Endpoints,
				CreatedAt: portalLink.CreatedAt.Time(),
				UpdatedAt: portalLink.UpdatedAt.Time(),
				DeletedAt: null.NewTime(portalLink.DeletedAt.Time(), true),
			}

			err = pgPortalLinkRepo.CreatePortalLink(ctx, postgresPortalLink)
			if err != nil {
				return fmt.Errorf("failed to save postgres portalLink: %v", err)
			}
		}

		pagination.Next = pager.Next
	}

	return nil
}
