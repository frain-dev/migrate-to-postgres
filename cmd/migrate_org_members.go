package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

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

func migrateOrgMemberCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting org members collection migration")
	defer fmt.Println("Finished org members collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.OrganisationMembersCollection)

	pgOrgMemberRepo := postgres.NewOrgMemberRepo(&PG{dbx: dbx})

	totalEndpoints, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count org members: %v", err)
	}

	numBatches := int(math.Ceil(float64(totalEndpoints) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var organisationMembers []datastore082.OrganisationMember

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &organisationMembers)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load organisationMembers: %v", err)
		}

		if len(organisationMembers) == 0 {
			break
		}
		lastID = organisationMembers[len(organisationMembers)-1].ID

		for i := range organisationMembers {
			orgMember := &organisationMembers[i]

			if !seen[orgMember.UID] {
				seen[orgMember.UID] = true
			} else {
				log.Errorf("org member %s returned multiple times", orgMember.UID)
				continue
			}

			var ok bool

			var projectID string
			if !util.IsStringEmpty(orgMember.Role.Project) {
				projectID, ok = oldIDToNewID[orgMember.Role.Project]
				if !ok {
					log.Errorf("new project id for project %s not found for org member %s", orgMember.Role.Project, orgMember.UID)
					continue
				}
			}

			var endpointID string
			if !util.IsStringEmpty(orgMember.Role.Endpoint) {
				endpointID, ok = oldIDToNewID[orgMember.Role.Endpoint]
				if !ok {
					log.Errorf("new endpoint id for endpoint %s not found for org member %s", orgMember.Role.Endpoint, orgMember.UID)
					continue
				}
			}

			orgID, ok := oldIDToNewID[orgMember.OrganisationID]
			if !ok {
				log.Errorf("new org id for org %s not found for org member %s", orgMember.OrganisationID, orgMember.UID)
				continue
			}

			userID, ok := oldIDToNewID[orgMember.UserID]
			if !ok {
				log.Errorf("new user id for user %s not found for org member  %s", orgMember.UserID, orgMember.UID)
				continue
			}

			postgresOrgMember := &datastore09.OrganisationMember{
				UID:            ulid.Make().String(),
				OrganisationID: orgID,
				UserID:         userID,
				Role: auth09.Role{
					Type:     auth09.RoleType(orgMember.Role.Type),
					Project:  projectID,
					Endpoint: endpointID,
				},
				CreatedAt: orgMember.CreatedAt.Time(),
				UpdatedAt: orgMember.UpdatedAt.Time(),
				DeletedAt: getDeletedAt(orgMember.DeletedAt),
			}

			err = pgOrgMemberRepo.CreateOrganisationMember(ctx, postgresOrgMember)
			if err != nil {
				return fmt.Errorf("failed to save postgres orgMember: %v", err)
			}

			oldIDToNewID[orgMember.UID] = postgresOrgMember.UID
		}
	}

	return nil
}
