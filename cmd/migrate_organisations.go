package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	cm "github.com/frain-dev/migrate-to-postgres/convoy082/datastore/mongo"
)

const (
	batchInsertOrganisations = `
	INSERT INTO convoy.organisations (
	        id, name, owner_id, custom_domain, 
	        assigned_domain, created_at, updated_at, deleted_at)
	VALUES (
	        :id, :name, :owner_id, :custom_domain, 
	       :created_at, :updated_at, :deleted_at :assigned_domain,
	        :created_at, :updated_at, :deleted_at
	);
    `
)

func migrateOrganisationsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.Background()
	orgRepo := cm.NewOrgRepo(store)
	pageable := datastore082.Pageable{
		Page:    0,
		PerPage: 1000,
		Sort:    0,
	}

	for {
		organisations, paginationData, err := orgRepo.LoadOrganisationsPaged(ctx, pageable)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load organisations: %v", err)
		}

		if len(organisations) == 0 {
			break
		}

		postgresOrgs := make([]datastore09.Organisation, len(organisations))

		for i := range organisations {
			org := &organisations[i]

			postgresOrgs[i] = datastore09.Organisation{
				UID:            ulid.Make().String(),
				OwnerID:        org.OwnerID,
				Name:           org.Name,
				CustomDomain:   null.NewString(org.CustomDomain, true),
				AssignedDomain: null.NewString(org.AssignedDomain, true),
				CreatedAt:      org.CreatedAt.Time(),
				UpdatedAt:      org.UpdatedAt.Time(),
				DeletedAt:      null.NewTime(org.DeletedAt.Time(), true),
			}
		}

		_, err = dbx.ExecContext(ctx, batchInsertOrganisations, &postgresOrgs)
		if err != nil {
			return fmt.Errorf("failed to batch insert organisations: %v", err)
		}

		pageable.Page = int(paginationData.Next)
	}

	return nil
}
