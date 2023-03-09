package main

import (
	"fmt"

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
		},
	}

	cmd.PersistentFlags().StringVar(&mongoDsn, "mongo-dsn", "", "Mongo database dsn")
	cmd.PersistentFlags().StringVar(&postgresDsn, "postgres-dsn", "", "Postgres database dsn")

	return cmd
}

func (p *PG) GetDB() *sqlx.DB {
	return p.dbx
}

type PG struct {
	dbx *sqlx.DB
}

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

	return nil
}
