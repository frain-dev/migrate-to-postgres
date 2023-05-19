package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jmoiron/sqlx"

	auth09 "github.com/frain-dev/convoy/auth"
	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateOrgInvitesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting org invites collection migration")
	defer fmt.Println("Finished org invites collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.OrganisationInvitesCollection)

	// pg := &PG{db: dbx}

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count org invites: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var organisationInvites []datastore082.OrganisationInvite

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &organisationInvites)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load organisationInvites: %v", err)
		}

		if len(organisationInvites) == 0 {
			break
		}
		lastID = organisationInvites[len(organisationInvites)-1].ID
		postgresOrgInvites := make([]*datastore09.OrganisationInvite, 0, len(organisationInvites))

		for i := range organisationInvites {
			orgInvite := &organisationInvites[i]

			if !seen[orgInvite.UID] {
				seen[orgInvite.UID] = true
			} else {
				log.Errorf("org invite %s returned multiple times", orgInvite.UID)
				continue
			}

			var ok bool

			var projectID string
			if !util.IsStringEmpty(orgInvite.Role.Project) {
				projectID, ok = oldIDToNewID[orgInvite.Role.Project]
				if !ok {
					log.Errorf("new project id for project %s not found for org invite %s", orgInvite.Role.Project, orgInvite.UID)
					continue
				}
			}

			var endpointID string
			if !util.IsStringEmpty(orgInvite.Role.Endpoint) {
				endpointID, ok = oldIDToNewID[orgInvite.Role.Endpoint]
				if !ok {
					log.Errorf("new endpoint id for endpoint %s not found for org invite %s", orgInvite.Role.Endpoint, orgInvite.UID)
					continue
				}
			}

			orgID, ok := oldIDToNewID[orgInvite.OrganisationID]
			if !ok {
				log.Errorf("new org id for org %s not found for org invite %s", orgInvite.OrganisationID, orgInvite.UID)
				continue
			}

			postgresOrgInvite := &datastore09.OrganisationInvite{
				UID:            orgInvite.UID,
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

			oldIDToNewID[orgInvite.UID] = postgresOrgInvite.UID
			postgresOrgInvites = append(postgresOrgInvites, postgresOrgInvite)
		}

		if len(postgresOrgInvites) > 0 {
			//err = pg.SaveOrganisationInvites(ctx, postgresOrgInvites)
			//if err != nil {
			//	return fmt.Errorf("failed to save postgres orgInvites: %v", err)
			//}
		}
	}

	return nil
}

const (
	createOrganisationInvite = `
	INSERT INTO convoy.organisation_invites (id, organisation_id, invitee_email, token, role_type, role_project, role_endpoint, status, expires_at, created_at, updated_at, deleted_at)
	VALUES (
	    :id, :organisation_id, :invitee_email, :token, :role_type,
	    :role_project, :role_endpoint, :status, :expires_at, :created_at, :updated_at, :deleted_at
	)
	`
)

func (i *PG) SaveOrganisationInvites(ctx context.Context, ivs []*datastore09.OrganisationInvite) error {
	values := make([]map[string]interface{}, 0, len(ivs))

	for _, iv := range ivs {
		var endpointID *string
		var projectID *string
		if !util.IsStringEmpty(iv.Role.Endpoint) {
			endpointID = &iv.Role.Endpoint
		}

		if !util.IsStringEmpty(iv.Role.Project) {
			projectID = &iv.Role.Project
		}

		values = append(values, map[string]interface{}{
			"id":              iv.UID,
			"organisation_id": iv.OrganisationID,
			"invitee_email":   iv.InviteeEmail,
			"token":           iv.Token,
			"role_type":       iv.Role.Type,
			"role_project":    projectID,
			"role_endpoint":   endpointID,
			"status":          iv.Status,
			"expires_at":      iv.ExpiresAt,
			"created_at":      iv.CreatedAt,
			"updated_at":      iv.UpdatedAt,
			"deleted_at":      iv.DeletedAt,
		})
	}

	_, err := i.db.NamedExecContext(ctx, createOrganisationInvite, values)
	return err
}
