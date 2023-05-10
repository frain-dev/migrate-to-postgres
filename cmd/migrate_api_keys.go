package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	auth09 "github.com/frain-dev/convoy/auth"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/jmoiron/sqlx"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

func migrateAPIKeysCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting api key collection migration")
	defer fmt.Println("Finished api key collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.APIKeyCollection)

	pg := &PG{db: dbx}

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count api keys: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var apiKeys []datastore082.APIKey

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &apiKeys)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load apiKeys: %v", err)
		}

		if len(apiKeys) == 0 {
			break
		}

		lastID = apiKeys[len(apiKeys)-1].ID
		postgresAPIKeys := make([]*datastore09.APIKey, 0, len(apiKeys))

		for i := range apiKeys {
			ak := &apiKeys[i]

			if !seen[ak.UID] {
				seen[ak.UID] = true
			} else {
				log.Errorf("api key %s returned multiple times", ak.UID)
				continue
			}

			var ok bool

			var projectID string
			if !util.IsStringEmpty(ak.Role.Project) {
				projectID, ok = oldIDToNewID[ak.Role.Project]
				if !ok {
					log.Errorf("new project id for project %s not found for api key %s", ak.Role.Project, ak.UID)
					continue
				}
			}

			var endpointID string
			if !util.IsStringEmpty(ak.Role.Endpoint) {
				endpointID, ok = oldIDToNewID[ak.Role.Endpoint]
				if !ok {
					log.Errorf("new endpoint id for endpoint %s not found for api key %s", ak.Role.Endpoint, ak.UID)
					continue
				}
			}

			var userID string
			if !util.IsStringEmpty(ak.UserID) {
				userID, ok = oldIDToNewID[ak.UserID]
				if !ok {
					log.Errorf("new user id for user %s not found for api key %s", ak.UserID, ak.UID)
					continue
				}
			}

			postgresAPIKey := &datastore09.APIKey{
				UID:    ak.UID,
				MaskID: ak.MaskID,
				Name:   ak.Name,
				Role: auth09.Role{
					Type:     auth09.RoleType(ak.Role.Type),
					Project:  projectID,
					Endpoint: endpointID,
				},
				Hash:      ak.Hash,
				Salt:      ak.Salt,
				Type:      datastore09.KeyType(ak.Type),
				UserID:    userID,
				ExpiresAt: null.TimeFrom(ak.ExpiresAt.Time()),
				CreatedAt: ak.CreatedAt.Time(),
				UpdatedAt: ak.UpdatedAt.Time(),
				DeletedAt: getDeletedAt(ak.DeletedAt),
			}

			oldIDToNewID[ak.UID] = postgresAPIKey.UID
			postgresAPIKeys = append(postgresAPIKeys, postgresAPIKey)
		}

		if len(postgresAPIKeys) > 0 {
			err = pg.SaveAPIKeys(ctx, postgresAPIKeys)
			if err != nil {
				return fmt.Errorf("failed to save postgres api keys: %v", err)
			}
		}
	}

	return nil
}

const (
	saveAPIKeys = `
    INSERT INTO convoy.api_keys (id,name,key_type,mask_id,role_type,role_project,role_endpoint,hash,salt,user_id,expires_at,created_at,updated_at, deleted_at)
    VALUES (
        :id, :name, :key_type, :mask_id, :role_type, :role_project,
        :role_endpoint, :hash, :salt, :user_id, :expires_at,
        :created_at, :updated_at, :deleted_at
    )
    `
)

func (a *PG) SaveAPIKeys(ctx context.Context, keys []*datastore09.APIKey) error {
	values := make([]map[string]interface{}, 0, len(keys))

	for _, key := range keys {
		var (
			userID     *string
			endpointID *string
			projectID  *string
			roleType   *auth09.RoleType
		)

		if !util.IsStringEmpty(key.UserID) {
			userID = &key.UserID
		}

		if !util.IsStringEmpty(key.Role.Endpoint) {
			endpointID = &key.Role.Endpoint
		}

		if !util.IsStringEmpty(key.Role.Project) {
			projectID = &key.Role.Project
		}

		if !util.IsStringEmpty(string(key.Role.Type)) {
			roleType = &key.Role.Type
		}

		values = append(values, map[string]interface{}{
			"id":            key.UID,
			"name":          key.Name,
			"key_type":      key.Type,
			"mask_id":       key.MaskID,
			"role_type":     roleType,
			"role_project":  projectID,
			"role_endpoint": endpointID,
			"hash":          key.Hash,
			"salt":          key.Salt,
			"user_id":       userID,
			"expires_at":    key.ExpiresAt,
			"created_at":    key.CreatedAt,
			"updated_at":    key.UpdatedAt,
			"deleted_at":    key.DeletedAt,
		})
	}

	_, err := a.db.NamedExecContext(ctx, saveAPIKeys, values)
	return err
}
