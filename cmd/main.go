package main

import (
	"os"
	_ "time/tzdata"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	err := os.Setenv("TZ", "") // Use UTC by default :)
	if err != nil {
		logrus.WithError(err).Fatal("failed to set TZ env")
	}
}

func main() {
	cmd := &cobra.Command{
		Use:     "Migrate-to-Postgres",
		Version: "0.1.0",
		Short:   "Migrate your convoy mongo database to postgres",
	}

	cmd.AddCommand(addMigrateCommand())

	err := cmd.Execute()
	if err != nil {
		logrus.Fatal(err)
	}
}
