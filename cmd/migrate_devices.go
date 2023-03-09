package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/oklog/ulid/v2"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/frain-dev/convoy/database/postgres"

	"github.com/jmoiron/sqlx"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

func migrateDevicesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.DeviceCollection)

	pgDeviceRepo := postgres.NewDeviceRepo(&PG{dbx: dbx})

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count portal links: %v", err)
	}

	var batchSize int64 = 1000
	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	pagination := datastore082.PaginationData{Next: 1}

	for i := 0; i < numBatches; i++ {
		var devices []datastore082.Device

		pager, err := store.FindMany(ctx, bson.M{}, nil, nil, pagination.Next, batchSize, &devices)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load devices: %v", err)
		}

		if len(devices) == 0 {
			break
		}

		for i := range devices {
			device := &devices[i]

			postgresDevice := &datastore09.Device{
				UID:        ulid.Make().String(),
				ProjectID:  device.ProjectID,
				EndpointID: device.EndpointID,
				HostName:   device.HostName,
				Status:     datastore09.DeviceStatus(device.Status),
				LastSeenAt: device.LastSeenAt.Time(),
				CreatedAt:  device.CreatedAt.Time(),
				UpdatedAt:  device.UpdatedAt.Time(),
				DeletedAt:  null.NewTime(device.DeletedAt.Time(), true),
			}

			err = pgDeviceRepo.CreateDevice(ctx, postgresDevice)
			if err != nil {
				return fmt.Errorf("failed to save postgres device: %v", err)
			}
		}

		pagination.Next = pager.Next
	}

	return nil
}
