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

	auth09 "github.com/frain-dev/convoy/auth"
	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

func migrateOrgMemberCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.OrganisationMembersCollection)

	pgOrgMemberRepo := postgres.NewOrgMemberRepo(&PG{dbx: dbx})

	totalEndpoints, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count projects: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(totalEndpoints) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 0; i < numBatches; i++ {
		var organisationMembers []datastore082.OrganisationMember

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &organisationMembers)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load organisationMembers: %v", err)
		}

		if len(organisationMembers) == 0 {
			break
		}

		for i := range organisationMembers {
			orgMember := &organisationMembers[i]

			postgresOrgMember := &datastore09.OrganisationMember{
				UID:            ulid.Make().String(),
				OrganisationID: orgMember.OrganisationID,
				UserID:         orgMember.UserID,
				Role: auth09.Role{
					Type:     auth09.RoleType(orgMember.Role.Type),
					Project:  orgMember.Role.Project,
					Endpoint: orgMember.Role.Endpoint,
				},
				CreatedAt: orgMember.CreatedAt.Time(),
				UpdatedAt: orgMember.UpdatedAt.Time(),
				DeletedAt: null.NewTime(orgMember.DeletedAt.Time(), true),
			}

			err = pgOrgMemberRepo.CreateOrganisationMember(ctx, postgresOrgMember)
			if err != nil {
				return fmt.Errorf("failed to save postgres orgMember: %v", err)
			}
		}

		pagination.Next = pager.Next
	}

	return nil
}
