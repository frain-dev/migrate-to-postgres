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

	pg := &PG{db: dbx}

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
		postgresOrgMembers := make([]*datastore09.OrganisationMember, 0, len(organisationMembers))

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

			oldIDToNewID[orgMember.UID] = postgresOrgMember.UID
			postgresOrgMembers = append(postgresOrgMembers, postgresOrgMember)
		}

		if len(postgresOrgMembers) > 0 {
			err = pg.SaveOrganisationMembers(ctx, postgresOrgMembers)
			if err != nil {
				return fmt.Errorf("failed to save postgres orgMembers: %v", err)
			}
		}
	}

	return nil
}

const (
	saveOrgMembers = `
	INSERT INTO convoy.organisation_members (id, organisation_id, user_id, role_type, role_project, role_endpoint, created_at, updated_at, deleted_at)
	VALUES (
	    :id, :organisation_id, :user_id, :role_type, :role_project,
	    :role_endpoint, :created_at, :updated_at, :deleted_at
	)
	`
)

func (o *PG) SaveOrganisationMembers(ctx context.Context, members []*datastore09.OrganisationMember) error {
	values := make([]map[string]interface{}, 0, len(members))

	for _, member := range members {
		var endpointID *string
		var projectID *string
		if !util.IsStringEmpty(member.Role.Endpoint) {
			endpointID = &member.Role.Endpoint
		}

		if !util.IsStringEmpty(member.Role.Project) {
			projectID = &member.Role.Project
		}

		values = append(values, map[string]interface{}{
			"id":              member.UID,
			"organisation_id": member.OrganisationID,
			"user_id":         member.UserID,
			"role_type":       member.Role.Type,
			"role_project":    projectID,
			"role_endpoint":   endpointID,
			"created_at":      member.CreatedAt,
			"updated_at":      member.UpdatedAt,
			"deleted_at":      member.DeletedAt,
		})
	}

	_, err := o.db.NamedExecContext(ctx, saveOrgMembers, values)
	return err
}
