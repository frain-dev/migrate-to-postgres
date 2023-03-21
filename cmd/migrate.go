package main

import (
	"fmt"
	"time"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"github.com/frain-dev/migrate-to-postgres/convoy082/config"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	cm "github.com/frain-dev/migrate-to-postgres/convoy082/datastore/mongo"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
)

func addMigrateCommand() *cobra.Command {
	var mongoDsn string
	var postgresDsn string
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Convoy migrations",
		Run: func(cmd *cobra.Command, args []string) {
			err := migrate(mongoDsn, postgresDsn)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Successfully migrated mongodb data to postgres")
		},
	}

	cmd.PersistentFlags().StringVar(&mongoDsn, "mongo-dsn", "", "Mongo database dsn")
	cmd.PersistentFlags().StringVar(&postgresDsn, "postgres-dsn", "", "Postgres database dsn")

	return cmd
}

var oldIDToNewID = map[string]string{}

func (p *PG) GetDB() *sqlx.DB {
	return p.db
}

type PG struct {
	db *sqlx.DB
}

const batchSize int64 = 5000

func migrate(mongoDsn, postgresDsn string) error {
	vv := config.Configuration{
		Database: config.DatabaseConfiguration{Dsn: mongoDsn},
	}

	mc, err := cm.New(vv)
	if err != nil {
		return err
	}

	store := datastore082.New(mc.Database())

	db, err := sqlx.Connect("postgres", postgresDsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	if err != nil {
		return err
	}

	err = migrateUserCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate user collection: %v", err)
	}

	err = migrateOrganisationsCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate organisations collection: %v", err)
	}

	err = migrateProjectsCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate projects collection: %v", err)
	}

	err = migrateEndpointsCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate endpoints collection: %v", err)
	}

	err = migrateOrgMemberCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate org member collection: %v", err)
	}

	err = migrateOrgInvitesCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate org invites collection: %v", err)
	}

	err = migratePortalLinksCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate portal links collection: %v", err)
	}

	err = migrateDevicesCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate devices collection: %v", err)
	}

	err = migrateConfigurationsCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate configurations collection: %v", err)
	}

	err = migrateSourcesCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate source collection: %v", err)
	}

	err = migrateSubscriptionsCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate subscriptions collection: %v", err)
	}

	err = migrateAPIKeysCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate api keys collection: %v", err)
	}

	err = migrateEventsCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate events collection: %v", err)
	}

	time.Sleep(time.Second * 10)

	err = migrateEventDeliveriesCollection(store, db)
	if err != nil {
		return fmt.Errorf("failed to migrate event deliveries collection: %v", err)
	}

	return nil
}
