package gospelmaria

import (
	"github.com/go-sql-driver/mysql"
)

const (
	mysqlDuplicateKey = 1062 // https://dev.mysql.com/doc/refman/5.7/en/error-messages-server.html#error_er_dup_entry
	mysqlDeadLock     = 1213 // https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_lock_deadlock
)

// isDeadlock returns true if err represents a MySQL deadlock condition.
func isDeadlock(err error) bool {
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == mysqlDeadLock
}

// isDuplicateEntry returns true if err represents a MySQL duplicate-key failure.
func isDuplicateEntry(err error) bool {
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == mysqlDuplicateKey
}
