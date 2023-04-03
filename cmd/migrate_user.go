package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/frain-dev/migrate-to-postgres/convoy082/pkg/log"

	"go.mongodb.org/mongo-driver/bson/primitive"

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

	pg := &PG{db: dbx}

	numBatches := int(math.Ceil(float64(count) / float64(batchSize)))
	var lastID primitive.ObjectID
	seen := map[string]bool{}

	// only migrate dojah users
	filter := bson.M{
		"uid": bson.M{
			"$in": []string{
				"43e2a10d-d3f4-4aeb-90a6-b696cace20c8",
				"c6c0b7e6-0aea-477b-864d-a57e3757d77a",
				"dbba1622-1ea9-486e-8a43-47e2d4443951",
			},
		},
	}

	for i := 1; i <= numBatches; i++ {
		var users []datastore082.User

		err = store.FindMany(ctx, filter, nil, nil, lastID, batchSize, &users)
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

		postgresUsers := make([]*datastore09.User, 0, len(users))

		for i := range users {
			user := &users[i]

			if !seen[user.UID] {
				seen[user.UID] = true
			} else {
				log.Errorf("user %s returned multiple times", user.UID)
				continue
			}

			postgresUser := datastore09.User{
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

			oldIDToNewID[user.UID] = postgresUser.UID
			postgresUsers = append(postgresUsers, &postgresUser)
		}

		if len(postgresUsers) > 0 {
			err = pg.SaveUsers(ctx, postgresUsers)
			if err != nil {
				return fmt.Errorf("failed to save postgres users: %v", err)
			}
		}
	}

	return nil
}

const (
	saveUsers = `
    INSERT INTO convoy.users (
		id,first_name,last_name,email,password,
        email_verified,reset_password_token, email_verification_token,
        reset_password_expires_at,email_verification_expires_at, created_at, updated_at, deleted_at)
    VALUES (
        :id, :first_name, :last_name, :email, :password,
        :email_verified, :reset_password_token, :email_verification_token,
        :reset_password_expires_at, :email_verification_expires_at, :created_at, :updated_at, :deleted_at
    )
    `
)

func (u *PG) SaveUsers(ctx context.Context, users []*datastore09.User) error {
	values := make([]map[string]interface{}, 0, len(users))

	for _, user := range users {
		values = append(values, map[string]interface{}{
			"id":                            user.UID,
			"first_name":                    user.FirstName,
			"last_name":                     user.LastName,
			"email":                         user.Email,
			"password":                      user.Password,
			"email_verified":                user.EmailVerified,
			"reset_password_token":          user.ResetPasswordToken,
			"email_verification_token":      user.EmailVerificationToken,
			"reset_password_expires_at":     user.ResetPasswordExpiresAt,
			"email_verification_expires_at": user.EmailVerificationExpiresAt,
			"created_at":                    user.CreatedAt,
			"updated_at":                    user.UpdatedAt,
			"deleted_at":                    user.DeletedAt,
		})
	}

	_, err := u.db.NamedExecContext(ctx, saveUsers, values)
	return err
}

func getDeletedAt(t *primitive.DateTime) null.Time {
	if t != nil {
		return null.TimeFrom(t.Time())
	}
	return null.Time{}
}
