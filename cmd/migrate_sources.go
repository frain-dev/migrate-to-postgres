package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/util"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jmoiron/sqlx"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

func migrateSourcesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting sources collection migration")
	defer fmt.Println("Finished sources collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.SourceCollection)

	pg := &PG{db: dbx}

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count sources: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var sources []datastore082.Source

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &sources)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load sources: %v", err)
		}

		if len(sources) == 0 {
			break
		}
		lastID = sources[len(sources)-1].ID
		postgresSources := make([]*datastore09.Source, 0, len(sources))

		for i := range sources {
			source := &sources[i]

			if !seen[source.UID] {
				seen[source.UID] = true
			} else {
				log.Errorf("source %s returned multiple times", source.UID)
				continue
			}

			projectID, ok := oldIDToNewID[source.ProjectID]
			if !ok {
				log.Errorf("new project id for project %s not found for source %s", source.ProjectID, source.UID)
				continue
			}

			postgresSource := &datastore09.Source{
				UID:            ulid.Make().String(),
				ProjectID:      projectID,
				MaskID:         source.MaskID,
				Name:           source.Name,
				Type:           datastore09.SourceType(source.Type),
				Provider:       datastore09.SourceProvider(source.Provider),
				IsDisabled:     source.IsDisabled,
				ForwardHeaders: source.ForwardHeaders,
				PubSub:         nil,
				CreatedAt:      source.CreatedAt.Time(),
				UpdatedAt:      source.UpdatedAt.Time(),
				DeletedAt:      getDeletedAt(source.DeletedAt),
			}

			if source.ProviderConfig != nil {
				postgresSource.ProviderConfig = &datastore09.ProviderConfig{}
				if source.ProviderConfig.Twitter != nil {
					postgresSource.ProviderConfig.Twitter.CrcVerifiedAt = null.TimeFrom(source.ProviderConfig.Twitter.CrcVerifiedAt.Time())
				}
			}

			if source.Verifier != nil {
				postgresSource.Verifier = &datastore09.VerifierConfig{
					Type: datastore09.VerifierType(source.Verifier.Type),
				}

				if source.Verifier.ApiKey != nil {
					postgresSource.Verifier.ApiKey = &datastore09.ApiKey{
						HeaderValue: source.Verifier.ApiKey.HeaderValue,
						HeaderName:  source.Verifier.ApiKey.HeaderName,
					}
				}

				if source.Verifier.HMac != nil {
					postgresSource.Verifier.HMac = &datastore09.HMac{
						Header:   source.Verifier.HMac.Header,
						Hash:     source.Verifier.HMac.Hash,
						Secret:   source.Verifier.HMac.Secret,
						Encoding: datastore09.EncodingType(source.Verifier.HMac.Encoding),
					}
				}

				if source.Verifier.BasicAuth != nil {
					postgresSource.Verifier.BasicAuth = &datastore09.BasicAuth{
						UserName: source.Verifier.BasicAuth.UserName,
						Password: source.Verifier.BasicAuth.Password,
					}
				}
			}

			oldIDToNewID[source.UID] = postgresSource.UID
			postgresSources = append(postgresSources, postgresSource)
		}

		if len(postgresSources) > 0 {
			err = pg.SaveSources(ctx, postgresSources)
			if err != nil {
				return fmt.Errorf("failed to save postgres source: %v", err)
			}
		}
	}

	return nil
}

const (
	saveSources = `
    INSERT INTO convoy.sources (id, source_verifier_id, name,type,mask_id,provider,is_disabled,forward_headers,project_id, pub_sub, created_at, updated_at, deleted_at)
    VALUES (
        :id, :source_verifier_id, :name, :type, :mask_id, :provider,
        :is_disabled, :forward_headers, :project_id, :pub_sub, :created_at, :updated_at, :deleted_at
    )
    `

	saveSourceVerifiers = `
    INSERT INTO convoy.source_verifiers (
        id,type,basic_username,basic_password,
        api_key_header_name,api_key_header_value,
        hmac_hash,hmac_header,hmac_secret,hmac_encoding
    )
    VALUES (
        :id,type, :basic_username, :basic_password,
        :api_key_header_name, :api_key_header_value,
        :hmac_hash, :hmac_header, :hmac_secret, :hmac_encoding
    )
    `
)

func (s *PG) SaveSources(ctx context.Context, sources []*datastore09.Source) error {
	sourceValues := make([]map[string]interface{}, 0, len(sources))
	sourceVerifierValues := make([]map[string]interface{}, 0, len(sources))

	for _, source := range sources {
		var (
			sourceVerifierID *string
			hmac             datastore09.HMac
			basic            datastore09.BasicAuth
			apiKey           datastore09.ApiKey
		)

		switch source.Verifier.Type {
		case datastore09.APIKeyVerifier:
			apiKey = *source.Verifier.ApiKey
		case datastore09.BasicAuthVerifier:
			basic = *source.Verifier.BasicAuth
		case datastore09.HMacVerifier:
			hmac = *source.Verifier.HMac
		}

		if !util.IsStringEmpty(string(source.Verifier.Type)) {
			source.VerifierID = ulid.Make().String()
			sourceVerifierID = &source.VerifierID

			sourceVerifierValues = append(sourceVerifierValues, map[string]interface{}{
				"id":                   sourceVerifierID,
				"type":                 source.Verifier.Type,
				"basic_username":       basic.UserName,
				"basic_password":       basic.Password,
				"api_key_header_name":  apiKey.HeaderName,
				"api_key_header_value": apiKey.HeaderValue,
				"hmac_hash":            hmac.Hash,
				"hmac_header":          hmac.Header,
				"hmac_secret":          hmac.Secret,
				"hmac_encoding":        hmac.Encoding,
			})
		}

		sourceValues = append(sourceValues, map[string]interface{}{
			"id":                 source.UID,
			"source_verifier_id": sourceVerifierID,
			"name":               source.Name,
			"type":               source.Type,
			"mask_id":            source.MaskID,
			"provider":           source.Provider,
			"is_disabled":        source.IsDisabled,
			"forward_headers":    source.ForwardHeaders,
			"project_id":         source.ProjectID,
			"pub_sub":            source.PubSub,
			"created_at":         source.CreatedAt,
			"updated_at":         source.UpdatedAt,
			"deleted_at":         source.DeletedAt,
		})
	}

	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	if len(sourceVerifierValues) > 0 {
		_, err = tx.NamedExecContext(ctx, saveSourceVerifiers, sourceVerifierValues)
		if err != nil {
			return err
		}
	}

	_, err = tx.NamedExecContext(ctx, saveSources, sourceValues)
	if err != nil {
		return err
	}

	return tx.Commit()
}
