package schema

import "database/sql"

// Create creates the gospel schema on the given database pool.
func Create(db *sql.DB) error {
	_, err := db.Exec(statements)
	return err
}
