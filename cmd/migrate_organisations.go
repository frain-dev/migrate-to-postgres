package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

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

	pg := &PG{db: dbx}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var organisations []datastore082.Organisation

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &organisations)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load organisations: %v", err)
		}

		if len(organisations) == 0 {
			break
		}
		lastID = organisations[len(organisations)-1].ID

		postgresOrgs := make([]*datastore09.Organisation, 0, len(organisations))

		for i := range organisations {
			org := &organisations[i]

			if !seen[org.UID] {
				seen[org.UID] = true
			} else {
				log.Errorf("org %s returned multiple times", org.UID)
				continue
			}

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

			oldIDToNewID[org.UID] = postgresOrg.UID
			postgresOrgs = append(postgresOrgs, postgresOrg)
		}

		if len(postgresOrgs) > 0 {
			err = pg.SaveOrganisations(ctx, postgresOrgs)
			if err != nil {
				return fmt.Errorf("failed to save postgres orgs: %v", err)
			}
		}
	}

	return nil
}

const (
	saveOrganizations = `
	INSERT INTO convoy.organisations (id, name, owner_id, custom_domain, assigned_domain, created_at, updated_at, deleted_at)
	VALUES (
	    :id, :name, :owner_id, :custom_domain, :assigned_domain, :created_at, :updated_at, :deleted_at
	)
	`
)

func (o *PG) SaveOrganisations(ctx context.Context, orgs []*datastore09.Organisation) error {
	values := make([]map[string]interface{}, 0, len(orgs))

	for _, org := range orgs {
		values = append(values, map[string]interface{}{
			"id":              org.UID,
			"name":            org.Name,
			"owner_id":        org.OwnerID,
			"custom_domain":   org.CustomDomain,
			"assigned_domain": org.AssignedDomain,
			"created_at":      org.CreatedAt,
			"updated_at":      org.UpdatedAt,
			"deleted_at":      org.DeletedAt,
		})
	}

	_, err := o.db.NamedExecContext(ctx, saveOrganizations, values)
	return err
}
