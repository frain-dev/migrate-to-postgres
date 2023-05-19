package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/convoy/config"
	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateProjectsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting projects collection migration")
	defer fmt.Println("Finished projects collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.ProjectsCollection)

	// pg := (&PG{db: dbx})

	totalProjects, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count projects: %v", err)
	}

	numBatches := int(math.Ceil(float64(totalProjects) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var projects []datastore082.Project

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &projects)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load projects: %v", err)
		}

		if len(projects) == 0 {
			break
		}
		lastID = projects[len(projects)-1].ID

		postgresProjects := make([]*datastore09.Project, 0, len(projects))

		for i := range projects {
			project := &projects[i]

			if !seen[project.UID] {
				seen[project.UID] = true
			} else {
				log.Errorf("project %s returned multiple times", project.UID)
				continue
			}

			orgID, ok := oldIDToNewID[project.OrganisationID]
			if !ok {
				log.Errorf("new org id for org %s not found for project %s", project.OrganisationID, project.UID)
				continue
			}

			postgresProject := &datastore09.Project{
				UID:             project.UID,
				Name:            project.Name,
				LogoURL:         project.LogoURL,
				OrganisationID:  orgID,
				ProjectConfigID: "",
				Type:            datastore09.ProjectType(project.Type),
				CreatedAt:       project.CreatedAt.Time(),
				UpdatedAt:       project.UpdatedAt.Time(),
				DeletedAt:       getDeletedAt(project.DeletedAt),
			}

			if project.Config == nil {
				postgresProject.Config = &datastore09.DefaultProjectConfig
			} else {
				postgresProject.Config = &datastore09.ProjectConfig{
					MaxIngestSize:            project.Config.MaxIngestSize,
					ReplayAttacks:            project.Config.ReplayAttacks,
					IsRetentionPolicyEnabled: project.Config.IsRetentionPolicyEnabled,
				}

				if project.Config.RetentionPolicy != nil {
					postgresProject.Config.RetentionPolicy = &datastore09.RetentionPolicyConfiguration{
						Policy: project.Config.RetentionPolicy.Policy,
					}
				} else {
					postgresProject.Config.RetentionPolicy = &datastore09.DefaultRetentionPolicy
				}

				if project.Config.RateLimit != nil {
					postgresProject.Config.RateLimit = &datastore09.RateLimitConfiguration{
						Count:    project.Config.RateLimit.Count,
						Duration: project.Config.RateLimit.Duration,
					}
				} else {
					postgresProject.Config.RateLimit = &datastore09.DefaultRateLimitConfig
				}

				if project.Config.Strategy != nil {
					postgresProject.Config.Strategy = &datastore09.StrategyConfiguration{
						Type:       datastore09.StrategyProvider(project.Config.Strategy.Type),
						Duration:   project.Config.Strategy.Duration,
						RetryCount: project.Config.Strategy.RetryCount,
					}
				} else {
					postgresProject.Config.Strategy = &datastore09.DefaultStrategyConfig
				}

				if project.Config.Signature != nil {
					postgresProject.Config.Signature = &datastore09.SignatureConfiguration{
						Header: config.SignatureHeaderProvider(project.Config.Signature.Header),
					}

					for _, version := range project.Config.Signature.Versions {
						postgresProject.Config.Signature.Versions = append(postgresProject.Config.Signature.Versions, datastore09.SignatureVersion{
							UID: version.UID,
							// UID: version.UID,
							Hash:      version.Hash,
							Encoding:  datastore09.EncodingType(version.Encoding),
							CreatedAt: version.CreatedAt.Time(),
						})
					}

				} else {
					postgresProject.Config.Signature = datastore09.GetDefaultSignatureConfig()
				}

			}

			if project.Metadata != nil {
				postgresProject.RetainedEvents = project.Metadata.RetainedEvents
			}

			oldIDToNewID[project.UID] = postgresProject.UID
			postgresProjects = append(postgresProjects, postgresProject)
		}

		if len(postgresProjects) > 0 {
			//err = pg.SaveProjects(ctx, postgresProjects)
			//if err != nil {
			//	return fmt.Errorf("failed to save postgres projects: %v", err)
			//}
		}
	}

	return nil
}

const (
	saveProjects = `
	INSERT INTO convoy.projects (id, name, type, logo_url, organisation_id, project_configuration_id, created_at, updated_at, deleted_at)
	VALUES (:id, :name, :type, :logo_url, :organisation_id, :project_configuration_id, :created_at, :updated_at, :deleted_at)
	`

	saveProjectConfigurations = `
	INSERT INTO convoy.project_configurations (
		id, retention_policy_policy, max_payload_read_size,
		replay_attacks_prevention_enabled,
		retention_policy_enabled, ratelimit_count,
		ratelimit_duration, strategy_type,
		strategy_duration, strategy_retry_count,
		signature_header, signature_versions
	  )
	  VALUES
		(
		:id, :retention_policy_policy, :max_payload_read_size,
		:replay_attacks_prevention_enabled,
		:retention_policy_enabled, :ratelimit_count,
		:ratelimit_duration, :strategy_type,
		:strategy_duration, :strategy_retry_count,
		:signature_header, :signature_versions
		)
	`
)

func (p *PG) SaveProjects(ctx context.Context, projects []*datastore09.Project) error {
	prValues := make([]map[string]interface{}, 0, len(projects))
	cfgs := make([]map[string]interface{}, 0, len(projects))

	for _, project := range projects {
		project.ProjectConfigID = ulid.Make().String()

		prValues = append(prValues, map[string]interface{}{
			"id":                       project.UID,
			"name":                     project.Name,
			"type":                     project.Type,
			"logo_url":                 project.LogoURL,
			"organisation_id":          project.OrganisationID,
			"project_configuration_id": project.ProjectConfigID,
			"created_at":               project.CreatedAt,
			"updated_at":               project.UpdatedAt,
			"deleted_at":               project.DeletedAt,
		})

		rc := project.Config.GetRetentionPolicyConfig()
		rlc := project.Config.GetRateLimitConfig()
		sc := project.Config.GetStrategyConfig()
		sgc := project.Config.GetSignatureConfig()

		cfgs = append(cfgs, map[string]interface{}{
			"id":                                project.ProjectConfigID,
			"retention_policy_policy":           rc.Policy,
			"max_payload_read_size":             project.Config.MaxIngestSize,
			"replay_attacks_prevention_enabled": project.Config.ReplayAttacks,
			"retention_policy_enabled":          project.Config.IsRetentionPolicyEnabled,
			"ratelimit_count":                   rlc.Count,
			"ratelimit_duration":                rlc.Duration,
			"strategy_type":                     sc.Type,
			"strategy_duration":                 sc.Duration,
			"strategy_retry_count":              sc.RetryCount,
			"signature_header":                  sgc.Header,
			"signature_versions":                sgc.Versions,
		})
	}

	tx, err := p.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer rollbackTx(tx)

	_, err = tx.NamedExecContext(ctx, saveProjectConfigurations, cfgs)
	if err != nil {
		return err
	}

	_, err = tx.NamedExecContext(ctx, saveProjects, prValues)
	if err != nil {
		return err
	}

	return tx.Commit()
}
