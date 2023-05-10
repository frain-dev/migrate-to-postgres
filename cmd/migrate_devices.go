package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jmoiron/sqlx"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	"go.mongodb.org/mongo-driver/mongo"
)

func migrateDevicesCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting device collection migration")
	defer fmt.Println("Finished device collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.DeviceCollection)

	pg := &PG{db: dbx}

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count devices: %v", err)
	}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var devices []datastore082.Device

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &devices)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load devices: %v", err)
		}

		if len(devices) == 0 {
			break
		}
		lastID = devices[len(devices)-1].ID

		postgresDevices := make([]*datastore09.Device, 0, len(devices))

		for i := range devices {
			device := &devices[i]

			if !seen[device.UID] {
				seen[device.UID] = true
			} else {
				log.Errorf("device %s returned multiple times", device.UID)
				continue
			}

			projectID, ok := oldIDToNewID[device.ProjectID]
			if !ok {
				log.Errorf("new project id for project %s not found for device %s", device.ProjectID, device.UID)
				continue
			}

			endpointID, ok := oldIDToNewID[device.EndpointID]
			if !ok {
				log.Errorf("new endpoint id for endpoint %s not found for device %s", device.EndpointID, device.UID)
				continue
			}

			postgresDevice := datastore09.Device{
				UID:        device.UID,
				ProjectID:  projectID,
				EndpointID: endpointID,
				HostName:   device.HostName,
				Status:     datastore09.DeviceStatus(device.Status),
				LastSeenAt: device.LastSeenAt.Time(),
				CreatedAt:  device.CreatedAt.Time(),
				UpdatedAt:  device.UpdatedAt.Time(),
				DeletedAt:  getDeletedAt(device.DeletedAt),
			}

			oldIDToNewID[device.UID] = postgresDevice.UID
			postgresDevices = append(postgresDevices, &postgresDevice)
		}

		if len(postgresDevices) > 0 {
			err = pg.SaveDevices(ctx, postgresDevices)
			if err != nil {
				return fmt.Errorf("failed to save postgres devices: %v", err)
			}
		}
	}

	return nil
}

const (
	saveDevices = `
	INSERT INTO convoy.devices (id, project_id, endpoint_id, host_name, status, last_seen_at, created_at, updated_at, deleted_at)
	VALUES (
	    :id, :project_id, :endpoint_id, :host_name, :status,
	    :last_seen_at, :created_at, :updated_at, :deleted_at
	)
	`
)

func (d *PG) SaveDevices(ctx context.Context, devices []*datastore09.Device) error {
	values := make([]map[string]interface{}, 0, len(devices))

	for _, device := range devices {
		values = append(values, map[string]interface{}{
			"id":           device.UID,
			"project_id":   device.ProjectID,
			"endpoint_id":  device.EndpointID,
			"host_name":    device.HostName,
			"status":       device.Status,
			"last_seen_at": device.LastSeenAt,
			"created_at":   device.CreatedAt,
			"updated_at":   device.UpdatedAt,
			"deleted_at":   device.DeletedAt,
		})
	}

	_, err := d.db.NamedExecContext(ctx, saveDevices, values)
	return err
}
