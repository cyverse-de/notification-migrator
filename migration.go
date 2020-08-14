package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	sq "github.com/Masterminds/squirrel"
)

// validateDestinationUsersTable verifies that the users table is empty in the destination database.
func validateDestinationUsersTable(destTx *sql.Tx) error {
	wrapMsg := "unable to verify that the destination users table is empty"

	// Obtain and validate the number of users in the database.
	row := destTx.QueryRow("SELECT count(*) FROM users")
	var userCount int
	err := row.Scan(&userCount)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}
	if userCount > 0 {
		return fmt.Errorf("the destination users table is not empty")
	}

	return nil
}

// migrateUsers migrates users to the destination database.
func migrateUsers(sourceTx, destTx *sql.Tx) error {
	wrapMsg := "user migration failed"

	// Verify that the users table in the destination database is empty.
	err := validateDestinationUsersTable(destTx)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Run the query to select the usernames from the source database.
	sourceRows, err := sourceTx.Query("SELECT username FROM users")
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}
	defer sourceRows.Close()

	// Begin the insertion statement.
	builder := sq.StatementBuilder.
		PlaceholderFormat(sq.Dollar).
		Insert("users").Columns("username")

	// Add the values to the insertion statement.
	for sourceRows.Next() {
		var username string
		err = sourceRows.Scan(&username)
		if err != nil {
			return errors.Wrap(err, wrapMsg)
		}
		builder = builder.Values(username)
	}

	// Generate the insertion statement and arguments.
	statement, args, err := builder.ToSql()
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Execute the insert statement.
	_, err = destTx.Exec(statement, args...)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	return nil
}

// validateDestinationNotifiationTypesTable verifies that the notification_types table in the
// destination database is empty.
func validateDestinationNotificationTypesTable(destTx *sql.Tx) error {
	wrapMsg := "unable to validate the destination notification_types table"

	// Obtain and validate the number of notification types in the database.
	row := destTx.QueryRow("SELECT count(*) FROM notification_types")
	var notificationTypeCount int
	err := row.Scan(&notificationTypeCount)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}
	if notificationTypeCount > 0 {
		return fmt.Errorf("the destination notification_types table is not empty")
	}

	return nil
}

// migrateNotificationTypes migrates existing notification types to the destination database.
func migrateNotificationTypes(sourceTx, destTx *sql.Tx) error {
	wrapMsg := "notification type migration failed"

	// Verify that the notification_types table in the destination database is empty.
	err := validateDestinationNotificationTypesTable(destTx)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Query the source database for existing notification types.
	sourceRows, err := sourceTx.Query("SELECT DISTINCT lower(type) FROM notifications")
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}
	defer sourceRows.Close()

	// Begin the insertion statement.
	builder := sq.StatementBuilder.
		PlaceholderFormat(sq.Dollar).
		Insert("notification_types").Columns("name")

	// Add each of the notification types to the insertion statement.
	for sourceRows.Next() {
		var notificationType string
		err = sourceRows.Scan(&notificationType)
		if err != nil {
			return errors.Wrap(err, wrapMsg)
		}
		builder = builder.Values(notificationType)
	}

	// Generate the insertion statement and arguments.
	statement, args, err := builder.ToSql()
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Execute the insert statement.
	_, err = destTx.Exec(statement, args...)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	return nil
}

// validateDestinationNotificationsTable verifies that the notifications table in the
// destination database is empty.
func validateDestinationNotificationsTable(destTx *sql.Tx) error {
	wrapMsg := "unable to validate the destination notifications table"

	// Obtain and validate the number of notifications in the database.
	row := destTx.QueryRow("SELECT count(*) FROM notifications")
	var notificationCount int
	err := row.Scan(&notificationCount)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}
	if notificationCount > 0 {
		return fmt.Errorf("the destination notifictions table is not empty")
	}

	return nil
}

// getNotificationTypeIDMap returns a map from notification type name to ID.
func getNotificationTypeIDMap(destTx *sql.Tx) (map[string]string, error) {
	wrapMsg := "unable to load the notification type ID map"

	// Execute the query.
	rows, err := destTx.Query("SELECT id, name FROM notification_types")
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}
	defer rows.Close()

	// Build the map.
	result := make(map[string]string)
	for rows.Next() {
		var id, name string
		err = rows.Scan(&id, &name)
		if err != nil {
			return nil, errors.Wrap(err, wrapMsg)
		}
		result[name] = id
	}

	return result, nil
}

// getUserIDMap returns a map from username to internal user ID.
func getUserIDMap(destTx *sql.Tx) (map[string]string, error) {
	wrapMsg := "unable to laod the user ID map"

	// Execute the query.
	rows, err := destTx.Query("SELECT id, username FROM users")
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}
	defer rows.Close()

	// Build the map.
	result := make(map[string]string)
	for rows.Next() {
		var id, username string
		err = rows.Scan(&id, &username)
		if err != nil {
			return nil, errors.Wrap(err, wrapMsg)
		}
		result[username] = id
	}

	return result, nil
}

// migrateNotifications migrates existing notifications to the destination database.
func migrateNotifications(sourceTx, destTx *sql.Tx) error {
	wrapMsg := "notification migration failed"

	// Verify that the destination notifications table is empty.
	err := validateDestinationNotificationsTable(destTx)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Obtain a map from notification type name to ID.
	notificationTypeIDFor, err := getNotificationTypeIDMap(destTx)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Obtain a map from user name to ID.
	userIDFor, err := getUserIDMap(destTx)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Run the query to retrieve the notifications from the source database.
	sourceQuery := `
		SELECT n.uuid,
			   lower(n.type),
			   u.username,
			   n.subject,
			   n.seen,
			   n.deleted,
			   n.date_created,
			   n.message
		FROM notifications n
		JOIN users u ON n.user_id = u.id
		ORDER BY n.date_created
	`
	sourceRows, err := sourceTx.Query(sourceQuery)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}
	defer sourceRows.Close()

	// Begin the insertion statement.
	base := sq.StatementBuilder.
		PlaceholderFormat(sq.Dollar).
		Insert("notifications").
		Columns(
			"id",
			"notification_type_id",
			"user_id",
			"subject",
			"seen",
			"deleted",
			"time_created",
			"incoming_json",
			"outgoing_json",
		)

	// insert each notification into the destination database. This is done one at a time to avoid argument limits.
	for sourceRows.Next() {
		var id, notificationType, username, subject, seen, deleted, timeCreated, message string

		// Retrieve the values for the current notification.
		err := sourceRows.Scan(&id, &notificationType, &username, &subject, &seen, &deleted, &timeCreated, &message)
		if err != nil {
			return errors.Wrap(err, wrapMsg)
		}
		notificationTypeID := notificationTypeIDFor[notificationType]
		userID := userIDFor[username]

		// Create an outgoing version of the message that includes the notification ID.
		var outgoing map[string]interface{}
		err = json.Unmarshal([]byte(message), &outgoing)
		if err != nil {
			return errors.Wrap(err, wrapMsg)
		}
		outgoing["message"].(map[string]interface{})["id"] = id
		outgoingJSON, err := json.Marshal(outgoing)
		if err != nil {
			return errors.Wrap(err, wrapMsg)
		}

		// Generate the insertion statement and arguments for this notification.
		builder := base.Values(
			id,
			notificationTypeID,
			userID,
			subject,
			seen,
			deleted,
			timeCreated,
			message,
			outgoingJSON,
		)
		query, args, err := builder.ToSql()
		if err != nil {
			return errors.Wrap(err, wrapMsg)
		}

		// Execute the statement.
		_, err = destTx.Exec(query, args...)
		if err != nil {
			return errors.Wrap(err, wrapMsg)
		}
	}

	return nil
}

// runMigration performs the actual database migration.
func runMigration(sourceTx, destTx *sql.Tx) error {
	wrapMsg := "database migration failed"

	// Migrate the users from the source database to the destination database.
	fmt.Println("Migrating users...")
	err := migrateUsers(sourceTx, destTx)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Migrate the notification types from the source database to the destnation database.
	fmt.Println("Migrating notification types...")
	err = migrateNotificationTypes(sourceTx, destTx)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Migrate the notifications from the source database to the destination database.
	fmt.Println("Migrating notifications...")
	err = migrateNotifications(sourceTx, destTx)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	return nil
}
