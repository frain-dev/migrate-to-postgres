package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/guregu/null.v4"

	datastore09 "github.com/frain-dev/convoy/datastore"
	datastore082 "github.com/frain-dev/migrate-to-postgres/convoy082/datastore"
	cm "github.com/frain-dev/migrate-to-postgres/convoy082/datastore/mongo"
)

const (
	batchInsertUser = `
    INSERT INTO convoy.users (
		id,first_name,last_name,email,password,
        email_verified,reset_password_token, email_verification_token,
        reset_password_expires_at,email_verification_expires_at,created_at,updated_at)
    VALUES (
            :id, :first_name, :last_name, :email, :password,
            :email_verified, :reset_password_token, :email_verification_token,
            :reset_password_expires_at, :email_verification_expires_at,
            :created_at, :updated_at, :deleted_at
        );
    `
)

func migrateUserCollection(store datastore082.Store, dbx *sqlx.DB) error {
	ctx := context.WithValue(context.Background(), datastore082.CollectionCtx, datastore082.UserCollection)
	userRepo := cm.NewUserRepo(store)
	pageable := datastore082.Pageable{
		Page:    0,
		PerPage: 1000,
		Sort:    0,
	}

	for {
		users, paginationData, err := userRepo.LoadUsersPaged(ctx, pageable)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}

			return fmt.Errorf("failed to load users: %v", err)
		}

		postgresUsers := make([]datastore09.User, len(users))

		for i := range users {
			user := &users[i]

			postgresUsers[i] = datastore09.User{
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
				DeletedAt:                  null.NewTime(user.DeletedAt.Time(), true),
				ResetPasswordExpiresAt:     user.ResetPasswordExpiresAt.Time(),
				EmailVerificationExpiresAt: user.EmailVerificationExpiresAt.Time(),
			}
		}

		_, err = dbx.ExecContext(ctx, batchInsertUser, &postgresUsers)
		if err != nil {
			return fmt.Errorf("failed to batch insert users: %v", err)
		}

		pageable.Page = int(paginationData.Next)
	}

	return nil
}
