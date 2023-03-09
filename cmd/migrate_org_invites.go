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

func migrateOrgInvitesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.OrganisationInvitesCollection)

	pgOrgMemberRepo := postgres.NewOrgInviteRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count projects: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 0; i < numBatches; i++ {
		var organisationInvites []datastore082.OrganisationInvite

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &organisationInvites)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load organisationInvites: %v", err)
		}

		if len(organisationInvites) == 0 {
			break
		}

		for i := range organisationInvites {
			orgInvite := &organisationInvites[i]

			postgresOrgInvite := &datastore09.OrganisationInvite{
				UID:            ulid.Make().String(),
				OrganisationID: orgInvite.OrganisationID,
				InviteeEmail:   orgInvite.InviteeEmail,
				Token:          orgInvite.Token,
				Role: auth09.Role{
					Type:     auth09.RoleType(orgInvite.Role.Type),
					Project:  orgInvite.Role.Project,
					Endpoint: orgInvite.Role.Endpoint,
				},
				Status:    datastore09.InviteStatus(orgInvite.Status),
				ExpiresAt: orgInvite.ExpiresAt.Time(),
				CreatedAt: orgInvite.CreatedAt.Time(),
				UpdatedAt: orgInvite.UpdatedAt.Time(),
				DeletedAt: null.NewTime(orgInvite.DeletedAt.Time(), true),
			}

			err = pgOrgMemberRepo.CreateOrganisationInvite(ctx, postgresOrgInvite)
			if err != nil {
				return fmt.Errorf("failed to save postgres orgInvite: %v", err)
			}
		}

		pagination.Next = pager.Next
	}

	return nil
}
