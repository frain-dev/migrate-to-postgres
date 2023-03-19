package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/convoy/config"
	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateProjectsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting projects collection migration")
	defer fmt.Println("Finished projects collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.ProjectsCollection)

	pgProjectRepo := postgres.NewProjectRepo(&PG{dbx: dbx})

	totalProjects, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count projects: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(totalProjects) / float64(batchSize)))

	for i := 1; i <= numBatches; i++ {
		var projects []datastore082.Project

		_, err = store.FindMany(ctx, bson.M{}, nil, nil, int64(i), batchSize, &projects)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load projects: %v", err)
		}

		if len(projects) == 0 {
			break
		}

		for i := range projects {
			project := &projects[i]

			orgID, ok := oldIDToNewID[project.OrganisationID]
			if !ok {
				log.Errorf("new org id for org %s not found for project %s", project.OrganisationID, project.UID)
				continue
			}

			postgresProject := &datastore09.Project{
				UID:             ulid.Make().String(),
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
							UID: ulid.Make().String(),
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

			err = pgProjectRepo.CreateProject(ctx, postgresProject)
			if err != nil {
				return fmt.Errorf("failed to save postgres project: %v", err)
			}

			oldIDToNewID[project.UID] = postgresProject.UID
		}
	}

	return nil
}
