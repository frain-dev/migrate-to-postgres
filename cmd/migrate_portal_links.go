package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migratePortalLinksCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting portal links collection migration")
	defer fmt.Println("Finisehd portal links collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.PortalLinkCollection)

	pgPortalLinkRepo := postgres.NewPortalLinkRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count portal links: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))

	for i := 1; i <= numBatches; i++ {
		var portalLinks []datastore082.PortalLink

		_, err = store.FindMany(ctx, bson.M{}, nil, nil, int64(i), batchSize, &portalLinks)
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

			projectID, ok := oldIDToNewID[portalLink.ProjectID]
			if !ok {
				log.Errorf("new project id for project %s not found for portal link %s", portalLink.ProjectID, portalLink.UID)
				continue
			}

			endpoints := make([]string, 0, len(portalLink.Endpoints))

			for _, id := range portalLink.Endpoints {
				if !util.IsStringEmpty(id) {
					newID, ok := oldIDToNewID[id]
					if !ok {
						log.Errorf("new endpoint id for endpoint %s not found for portal link %s", id, portalLink.UID)
						continue
					}

					endpoints = append(endpoints, newID)
				}
			}

			postgresPortalLink := &datastore09.PortalLink{
				UID:       ulid.Make().String(),
				Name:      portalLink.Name,
				ProjectID: projectID,
				Token:     portalLink.Token,
				Endpoints: endpoints,
				CreatedAt: portalLink.CreatedAt.Time(),
				UpdatedAt: portalLink.UpdatedAt.Time(),
				DeletedAt: getDeletedAt(portalLink.DeletedAt),
			}

			err = pgPortalLinkRepo.CreatePortalLink(ctx, postgresPortalLink)
			if err != nil {
				return fmt.Errorf("failed to save postgres portalLink: %v", err)
			}

			oldIDToNewID[portalLink.UID] = postgresPortalLink.UID
		}
	}

	return nil
}
