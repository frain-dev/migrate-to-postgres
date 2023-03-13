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

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
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

	for i := 1; i <= numBatches; i++ {
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

			projectID, ok := oldIDToNewID[device.ProjectID]
			if !ok {
				return fmt.Errorf("new project id for project %s not found for device %s", device.ProjectID, device.UID)
			}

			endpointID, ok := oldIDToNewID[device.EndpointID]
			if !ok {
				return fmt.Errorf("new endpoint id for endpoint %s not found for device %s", device.EndpointID, device.UID)
			}

			postgresDevice := &datastore09.Device{
				UID:        ulid.Make().String(),
				ProjectID:  projectID,
				EndpointID: endpointID,
				HostName:   device.HostName,
				Status:     datastore09.DeviceStatus(device.Status),
				LastSeenAt: device.LastSeenAt.Time(),
				CreatedAt:  device.CreatedAt.Time(),
				UpdatedAt:  device.UpdatedAt.Time(),
				DeletedAt:  getDeletedAt(device.DeletedAt),
			}

			err = pgDeviceRepo.CreateDevice(ctx, postgresDevice)
			if err != nil {
				return fmt.Errorf("failed to save postgres device: %v", err)
			}

			oldIDToNewID[device.UID] = postgresDevice.UID
		}

		pagination.Next = pager.Next
	}

	return nil
}
