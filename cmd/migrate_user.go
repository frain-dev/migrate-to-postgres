package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/frain-dev/convoy/database/postgres"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/jmoiron/sqlx"

	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
)

func migrateUserCollection(store datastore082.Store, dbx *sqlx.DB) error {
	fmt.Println("Starting user collection migration")
	defer fmt.Println("Finished user collection migration")

	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.UserCollection)

	count, err := store.Count(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("faild to count users: %v", err)
	}

	pgUserRepo := postgres.NewUserRepo(&PG{dbx: dbx})

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	for i := 1; i <= numBatches; i++ {
		var users []datastore082.User

		err = store.FindMany(ctx, bson.M{}, nil, nil, lastID, batchSize, &users)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load users: %v", err)
		}

		if len(users) == 0 {
			break
		}
		lastID = users[len(users)-1].ID

		for i := range users {
			user := &users[i]

			if !seen[user.UID] {
				seen[user.UID] = true
			} else {
				log.Errorf("user %s returned multiple times", user.UID)
				continue
			}

			postgresUser := &datastore09.User{
				UID:                        ulid.Make().String(),
				FirstName:                  user.FirstName,
				LastName:                   user.LastName,
				Email:                      user.Email,
				EmailVerified:              user.EmailVerified,
				Password:                   user.Password,
				ResetPasswordToken:         user.ResetPasswordToken,
				EmailVerificationToken:     user.EmailVerificationToken,
				CreatedAt:                  user.CreatedAt.Time(),
				UpdatedAt:                  user.UpdatedAt.Time(),
				DeletedAt:                  getDeletedAt(user.DeletedAt),
				ResetPasswordExpiresAt:     user.ResetPasswordExpiresAt.Time(),
				EmailVerificationExpiresAt: user.EmailVerificationExpiresAt.Time(),
			}

			err = pgUserRepo.CreateUser(ctx, postgresUser)
			if err != nil {
				fmt.Printf("user %+v\n", postgresUser)
				return fmt.Errorf("failed to save postgres user: %v", err)
			}

			oldIDToNewID[user.UID] = postgresUser.UID
		}
	}

	return nil
}

func getDeletedAt(t *primitive.DateTime) null.Time {
	if t != nil {
		return null.TimeFrom(t.Time())
	}
	return null.Time{}
}
