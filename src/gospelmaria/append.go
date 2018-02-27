package gospelmaria

import (
	"context"
	"database/sql"

	"github.com/jmalloc/gospel/src/gospel"
)

// atomicAppend writes events to a stream inside a transaction using the given
// append strategy.
func atomicAppend(
	ctx context.Context,
	db *sql.DB,
	storeID uint64,
	addr *gospel.Address,
	events []gospel.Event,
	strategy appendStrategy,
) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := strategy(ctx, tx, storeID, addr, events); err != nil {
		return err
	}

	return tx.Commit()
}

// appendStrategy is a function that actually performs the database queries
// to write events.
//
// addr.Offset is updated to refer to the next unused offset after the append.
type appendStrategy func(
	ctx context.Context,
	tx *sql.Tx,
	storeID uint64,
	addr *gospel.Address,
	events []gospel.Event,
) error

// appendChecked is an append strategy which verifies that addr refers to the
// next unused offset.
func appendChecked(
	ctx context.Context,
	tx *sql.Tx,
	storeID uint64,
	addr *gospel.Address,
	events []gospel.Event,
) error {
	for _, ev := range events {
		row := tx.QueryRowContext(
			ctx,
			`SELECT append_checked(?, ?, ?, ?, ?, ?)`,
			storeID,
			addr.Stream,
			addr.Offset,
			ev.EventType,
			ev.ContentType,
			ev.Body,
		)

		var ok bool
		if err := row.Scan(&ok); err != nil {
			return err
		}

		if !ok {
			return gospel.ConflictError{
				Addr:  *addr,
				Event: ev,
			}
		}

		addr.Offset++
	}

	return nil
}

// appendUnchecked is an append strategy which always appends regardless
// of the offset in addr.
func appendUnchecked(
	ctx context.Context,
	tx *sql.Tx,
	storeID uint64,
	addr *gospel.Address,
	events []gospel.Event,
) error {
	for _, ev := range events {
		row := tx.QueryRowContext(
			ctx,
			`SELECT append_unchecked(?, ?, ?, ?, ?)`,
			storeID,
			addr.Stream,
			ev.EventType,
			ev.ContentType,
			ev.Body,
		)

		if err := row.Scan(&addr.Offset); err != nil {
			return err
		}
	}

	addr.Offset++

	return nil
}
