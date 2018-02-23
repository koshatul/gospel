package mariadb

import (
	"github.com/go-sql-driver/mysql"
)

const (
	// https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html#error_er_lock_deadlock
	mysqlDeadLock = 1213
)

// isDeadlock returns true if err is an mysqlDeadLock MySQL error.
func isDeadlock(err error) bool {
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == mysqlDeadLock
}
