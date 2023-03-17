package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	auth09 "github.com/frain-dev/convoy/auth"
	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateOrgInvitesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting org invites collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.OrganisationInvitesCollection)

	pgOrgMemberRepo := postgres.NewOrgInviteRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count org invites: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))

	for i := 1; i <= numBatches; i++ {
		var organisationInvites []datastore082.OrganisationInvite

		_, err = store.FindMany(ctx, bson.M{}, nil, nil, int64(i), batchSize, &organisationInvites)
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

			projectID, ok := oldIDToNewID[orgInvite.Role.Project]
			if !ok {
				return fmt.Errorf("new project id for project %s not found for org invite %s", orgInvite.Role.Project, orgInvite.UID)
			}

			var endpointID string
			if !util.IsStringEmpty(orgInvite.Role.Endpoint) {
				endpointID, ok = oldIDToNewID[orgInvite.Role.Endpoint]
				if !ok {
					return fmt.Errorf("new endpoint id for endpoint %s not found for org invite %s", orgInvite.Role.Endpoint, orgInvite.UID)
				}
			}

			orgID, ok := oldIDToNewID[orgInvite.OrganisationID]
			if !ok {
				return fmt.Errorf("new org id for org %s not found for org invite %s", orgInvite.OrganisationID, orgInvite.UID)
			}

			postgresOrgInvite := &datastore09.OrganisationInvite{
				UID:            ulid.Make().String(),
				OrganisationID: orgID,
				InviteeEmail:   orgInvite.InviteeEmail,
				Token:          orgInvite.Token,
				Role: auth09.Role{
					Type:     auth09.RoleType(orgInvite.Role.Type),
					Project:  projectID,
					Endpoint: endpointID,
				},
				Status:    datastore09.InviteStatus(orgInvite.Status),
				ExpiresAt: orgInvite.ExpiresAt.Time(),
				CreatedAt: orgInvite.CreatedAt.Time(),
				UpdatedAt: orgInvite.UpdatedAt.Time(),
				DeletedAt: getDeletedAt(orgInvite.DeletedAt),
			}

			err = pgOrgMemberRepo.CreateOrganisationInvite(ctx, postgresOrgInvite)
			if err != nil {
				return fmt.Errorf("failed to save postgres orgInvite: %v", err)
			}

			oldIDToNewID[orgInvite.UID] = postgresOrgInvite.UID
		}
	}

	fmt.Println("Finished org invites collection migration")
	return nil
}
