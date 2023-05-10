package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

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

	pg := &PG{db: dbx}

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count portal links: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var portalLinks []datastore082.PortalLink

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &portalLinks)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load portalLinks: %v", err)
		}

		if len(portalLinks) == 0 {
			break
		}
		lastID = portalLinks[len(portalLinks)-1].ID
		postgresPortalLinks := make([]*datastore09.PortalLink, 0, len(portalLinks))

		for i := range portalLinks {
			portalLink := &portalLinks[i]

			if !seen[portalLink.UID] {
				seen[portalLink.UID] = true
			} else {
				log.Errorf("portal link %s returned multiple times", portalLink.UID)
				continue
			}

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
				UID:       portalLink.UID,
				Name:      portalLink.Name,
				ProjectID: projectID,
				Token:     portalLink.Token,
				Endpoints: endpoints,
				CreatedAt: portalLink.CreatedAt.Time(),
				UpdatedAt: portalLink.UpdatedAt.Time(),
				DeletedAt: getDeletedAt(portalLink.DeletedAt),
			}

			oldIDToNewID[portalLink.UID] = postgresPortalLink.UID
			postgresPortalLinks = append(postgresPortalLinks, postgresPortalLink)
		}

		if len(postgresPortalLinks) > 0 {
			err = pg.SavePortalLinks(ctx, postgresPortalLinks)
			if err != nil {
				return fmt.Errorf("failed to save postgres portalLinks: %v", err)
			}
		}
	}

	return nil
}

const (
	savePortalLinks = `
	INSERT INTO convoy.portal_links (id, project_id, name, token, endpoints, created_at, updated_at, deleted_at)
	VALUES (:id, :project_id, :name, :token, :endpoints, :created_at, :updated_at, :deleted_at)
	`

	savePortalLinkEndpoints = `
	INSERT INTO convoy.portal_links_endpoints (portal_link_id, endpoint_id) VALUES (:portal_link_id, :endpoint_id)
	`
)

func (p *PG) SavePortalLinks(ctx context.Context, portals []*datastore09.PortalLink) error {
	pl := make([]map[string]interface{}, 0, len(portals))
	plEndpoints := make([]postgres.PortalLinkEndpoint, 0, len(portals)*2)

	for _, portal := range portals {
		pl = append(pl, map[string]interface{}{
			"id":         portal.UID,
			"project_id": portal.ProjectID,
			"name":       portal.Name,
			"token":      portal.Token,
			"endpoints":  portal.Endpoints,
			"created_at": portal.CreatedAt,
			"updated_at": portal.CreatedAt,
			"deleted_at": portal.DeletedAt,
		})

		if len(portal.Endpoints) > 0 {
			for _, endpointID := range portal.Endpoints {
				plEndpoints = append(plEndpoints, postgres.PortalLinkEndpoint{PortalLinkID: portal.UID, EndpointID: endpointID})
			}
		}
	}

	tx, err := p.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	_, err = tx.NamedExecContext(ctx, savePortalLinks, pl)
	if err != nil {
		return err
	}

	_, err = tx.NamedExecContext(ctx, savePortalLinkEndpoints, plEndpoints)
	if err != nil {
		return err
	}

	return tx.Commit()
}
