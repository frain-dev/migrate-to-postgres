package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	cm "github.com/frain-dev/migrate-to-postgres/convoy082/datastore/mongo"
)

const (
	batchInsertOrganisations = `
	INSERT INTO convoy.organisations (
	        id, name, owner_id, custom_domain, 
	        assigned_domain, created_at, updated_at, deleted_at)
	VALUES (
	        :id, :name, :owner_id, :custom_domain, 
	       :created_at, :updated_at, :deleted_at :assigned_domain,
	        :created_at, :updated_at, :deleted_at
	);
    `
)

func migrateProjectsCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.ProjectsCollection)

	projectRepo := cm.NewProjectRepo(store)
	pgProjectRepo := postgres.NewProjectRepo(&PG{dbx: dbx})
	pageable := datastore082.Pageable{
		Page:    0,
		PerPage: 1000,
		Sort:    0,
	}

	totalProjects, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count projects: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(totalProjects) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 0; i < numBatches; i++ {
		var projects []datastore082.Project

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, int64(i), batchSize, &projects)
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

			postgresProject := &datastore09.Project{
				UID:             ulid.Make().String(),
				Name:            project.Name,
				LogoURL:         project.LogoURL,
				OrganisationID:  project.OrganisationID,
				ProjectConfigID: "",
				Type:            datastore09.ProjectType(project.Type),
				Config: &datastore09.ProjectConfig{
					MaxIngestSize:            project.Config.MaxIngestSize,
					ReplayAttacks:            project.Config.ReplayAttacks,
					IsRetentionPolicyEnabled: project.Config.IsRetentionPolicyEnabled,
					RetentionPolicy: &datastore09.RetentionPolicyConfiguration{
						Policy: project.Config.RetentionPolicy.Policy,
					},
					RateLimit: &datastore09.RateLimitConfiguration{
						Count:    project.Config.RateLimit.Count,
						Duration: project.Config.RateLimit.Duration,
					},
					Strategy: &datastore09.StrategyConfiguration{
						Type:       datastore09.StrategyProvider(project.Config.Strategy.Type),
						Duration:   0,
						RetryCount: 0,
					},
					Signature: &datastore09.SignatureConfiguration{
						Hash:     "",
						Header:   "",
						Versions: nil,
					},
				},
				Statistics: &datastore09.ProjectStatistics{
					MessagesSent:   0,
					TotalEndpoints: 0,
				},
				RetainedEvents: 0,
				CreatedAt:      time.Time{},
				UpdatedAt:      time.Time{},
				DeletedAt: null.Time{
					NullTime: sql.NullTime{
						Time:  time.Time{},
						Valid: false,
					},
				},
			}

			if project.Config == nil {
				project.Config = &datastore09.DefaultProjectConfig
			}

			err = pgProjectRepo.CreateProject(ctx, postgresProject)
			if err != nil {
				return fmt.Errorf("failed to save postgres project: %v", err)
			}
		}

		pagination.Next = pager.Next
	}

	return nil
}
