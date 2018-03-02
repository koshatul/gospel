package schema

var statements = `
CREATE PROCEDURE IF NOT EXISTS alter_partitions
(
    p_table VARCHAR(255)
)
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    -- Use UTC explicitly to prevent any ambiguity as to which partition
    -- should contain rows that are inserted near the 1st of each month.
    DECLARE v_now   TIMESTAMP DEFAULT UTC_TIMESTAMP;
    DECLARE v_year  INTEGER DEFAULT YEAR(v_now);
    DECLARE v_month INTEGER DEFAULT MONTH(v_now);

    DECLARE v_threshold_curr BIGINT;
    DECLARE v_threshold_next BIGINT;

    -- @threshold_[curr|next] are the Unix timestamps used in the range
    -- partition for the current month and next month, respectively.
    --
    -- Note that because partitions are defined by their UPPER BOUND, the
    -- thresholds refer to the next month (v_month + 1) and the month after
    -- that (v_month + 2).
    --
    -- We have to use CONVERT_TZ() because UNIX_TIMESTAMP() always assumes the
    -- string it's parsing is in the server/session timezone (even if another
    -- timezone is specified in the string).
    SET v_threshold_curr = CONVERT_TZ(
        CONCAT(v_year, '-', v_month+1, '-01 00:00:00'),
        'UTC',
        @@session.time_zone
    );

    SET v_threshold_next = CONVERT_TZ(
        CONCAT(v_year, '-', v_month+2, '-01 00:00:00'),
        'UTC',
        @@session.time_zone
    );

    IF v_threshold_curr IS NULL OR v_threshold_next IS NULL THEN
        SIGNAL SQLSTATE '45000' SET
            MESSAGE_TEXT='timezone conversion failed, perhaps timezone information is not loaded';
    END IF;

    PREPARE st FROM CONCAT(
        'ALTER TABLE ', p_table, '
        ADD PARTITION IF NOT EXISTS (
            PARTITION P_', v_year, '_', LPAD(v_month+0, 2, '0'), ' VALUES LESS THAN (', UNIX_TIMESTAMP(v_threshold_curr), '),
            PARTITION P_', v_year, '_', LPAD(v_month+1, 2, '0'), ' VALUES LESS THAN (', UNIX_TIMESTAMP(v_threshold_next), ')
        )'
    );
    EXECUTE st;
    DEALLOCATE PREPARE st;
END;
--
-- append_checked records a new fact to a named stream and to the ε-stream.
--
-- It returns TRUE if the event is appended, or FALSE if there is a conflict.
-- A conflict occurs when p_offset is not the next unused offset of p_stream.
--
CREATE FUNCTION IF NOT EXISTS append_checked
(
    p_store_id     BIGINT UNSIGNED,
    p_stream       VARBINARY(255),
    p_offset       BIGINT UNSIGNED,
    p_event_type   VARBINARY(255),
    p_content_type VARBINARY(255),
    p_body         LONGBLOB
)
RETURNS BOOLEAN
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_now TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6);
    DECLARE v_event_id BIGINT UNSIGNED;

    IF p_stream = "" THEN
        SIGNAL SQLSTATE '45000' SET
            MESSAGE_TEXT='cannot append directly to the ε-stream';
    END IF;

    -- If p_offset = 0, we're attempting to create the stream.
    IF p_offset = 0 THEN

        BEGIN
            DECLARE EXIT HANDLER FOR 1062 RETURN FALSE; -- 1062 = duplicate key

            INSERT INTO stream SET
                store_id = p_store_id,
                name     = p_stream,
                next     = 1;
        END;

        CALL record_stream_created(v_now, p_store_id, p_stream);

    -- Otherwise the stream must already exist at p_offset.
    ELSE

        UPDATE stream SET
            next = p_offset + 1
        WHERE store_id = p_store_id
            AND name = p_stream
            AND next = p_offset;

        IF ROW_COUNT() != 1 THEN
            RETURN FALSE;
        END IF;

    END IF;

    -- Once we know our write will not conflict, we can store the event.
    SET v_event_id = store_event(
        v_now,
        p_store_id,
        p_event_type,
        p_content_type,
        p_body
    );

    -- Record a fact on the ε-stream.
    CALL record_epsilon(v_now, p_store_id, v_event_id);

    -- Record a fact on the named stream.
    INSERT INTO fact SET
        store_id = p_store_id,
        stream   = p_stream,
        offset   = p_offset,
        event_id = v_event_id,
        time     = v_now;

    RETURN TRUE;
END;
--
-- append_unchecked records a new fact to a named stream and to the ε-stream.
--
CREATE FUNCTION IF NOT EXISTS append_unchecked
(
    p_store_id     BIGINT UNSIGNED,
    p_stream       VARBINARY(255),
    p_event_type   VARBINARY(255),
    p_content_type VARBINARY(255),
    p_body         LONGBLOB
)
RETURNS BIGINT UNSIGNED
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_now TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6);
    DECLARE v_event_id BIGINT UNSIGNED;
    DECLARE v_offset BIGINT UNSIGNED;

    IF p_stream = "" THEN
        SIGNAL SQLSTATE '45000' SET
            MESSAGE_TEXT='cannot append directly to the ε-stream';
    END IF;

    -- Record the event first, defering the locking of the stream table as late
    -- as possible.
    SET v_event_id = store_event(
        v_now,
        p_store_id,
        p_event_type,
        p_content_type,
        p_body
    );

    -- We don't care what the offset is now, we just want to find it so we can
    -- add our fact.
    INSERT INTO stream SET
        store_id = p_store_id,
        name     = p_stream,
        next     = 1
    ON DUPLICATE KEY UPDATE
        next = next + 1;

    -- Our offset is whatever we set the next offset to minus 1.
    SELECT next - 1
        INTO v_offset
        FROM stream
    WHERE store_id = p_store_id
        AND name   = p_stream;

    IF v_offset = 0 THEN
        CALL record_stream_created(v_now, p_store_id, p_stream);
    END IF;

    -- Record our fact on the ε-stream.
    CALL record_epsilon(v_now, p_store_id, v_event_id);

    -- Record our fact on the named stream.
    INSERT INTO fact SET
        store_id = p_store_id,
        stream   = p_stream,
        offset   = v_offset,
        event_id = v_event_id,
        time     = v_now;

    RETURN v_offset;
END;
--
-- open_store returns the ID of the store named p_store, creating it if it does
-- not already exist.
--
CREATE FUNCTION IF NOT EXISTS open_store
(
    p_store VARBINARY(255)
)
RETURNS BIGINT UNSIGNED
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_now TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6);
    DECLARE v_store_id BIGINT UNSIGNED;
    DECLARE v_exists BOOLEAN DEFAULT FALSE;

    DECLARE CONTINUE HANDLER FOR 1062 SET v_exists = TRUE; -- 1062 = duplicate key

    INSERT INTO store SET
        name = p_store;

    SELECT id
        INTO v_store_id
        FROM store
    WHERE
        name = p_store;

    -- Create the ε-stream and record an event about store creation.
    IF v_exists = FALSE THEN
        INSERT INTO stream SET
            store_id = v_store_id,
            name     = "",
            next     = 0;

        CALL record_store_created(v_now, v_store_id, p_store);
    END IF;

    RETURN v_store_id;
END;
--
-- record_epsilon unconditionally records a fact to the ε-stream.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE PROCEDURE IF NOT EXISTS record_epsilon
(
    p_now      TIMESTAMP(6),
    p_store_id BIGINT UNSIGNED,
    p_event_id BIGINT UNSIGNED
)
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_offset BIGINT UNSIGNED;

    SELECT next
        INTO v_offset
        FROM stream
    WHERE store_id = p_store_id
        AND name = ""
        FOR UPDATE;

    IF v_offset IS NULL THEN
        SIGNAL SQLSTATE '45000' SET
            MESSAGE_TEXT='ε-stream does not exist';
    END IF;

    INSERT INTO fact SET
        store_id = p_store_id,
        stream   = "",
        offset   = v_offset,
        event_id = p_event_id,
        time     = p_now;

    UPDATE stream SET
        next = v_offset + 1
    WHERE store_id = p_store_id
        AND name = "";
END;
--
-- record_store_created records a fact to the ε-stream about a new store being
-- created.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE PROCEDURE IF NOT EXISTS record_store_created
(
    p_now      TIMESTAMP(6),
    p_store_id BIGINT UNSIGNED,
    p_store    VARBINARY(255)
)
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    CALL record_epsilon(
        p_now,
        p_store_id,
        store_event(
            p_now,
            p_store_id,
            "$store.created",
            "application/vnd.gospel.store.created.v1",
            p_store
        )
    );
END;
--
-- record_stream_created records a fact to the ε-stream about a new named stream
-- being created.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE PROCEDURE IF NOT EXISTS record_stream_created
(
    p_now      TIMESTAMP(6),
    p_store_id BIGINT UNSIGNED,
    p_stream   VARBINARY(255)
)
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    CALL record_epsilon(
        p_now,
        p_store_id,
        store_event(
            p_now,
            p_store_id,
            "$stream.created",
            "application/vnd.gospel.stream.created.v1",
            p_stream
        )
    );
END;
--
-- store_event inserts an event and returns its auto-increment ID.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE FUNCTION IF NOT EXISTS store_event
(
    p_now          TIMESTAMP(6),
    p_store_id     BIGINT UNSIGNED,
    p_event_type   VARBINARY(255),
    p_content_type VARBINARY(255),
    p_body         LONGBLOB
)
RETURNS BIGINT UNSIGNED
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    INSERT INTO event SET
        time         = p_now,
        store_id     = p_store_id,
        event_type   = p_event_type,
        content_type = p_content_type,
        body         = p_body;

    RETURN LAST_INSERT_ID();
END;
--
-- event contains application-defined event data.
--
CREATE TABLE IF NOT EXISTS event
(
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    time         TIMESTAMP(6) NOT NULL,

    store_id     BIGINT UNSIGNED NOT NULL,
    event_type   VARBINARY(255) NOT NULL,
    content_type VARBINARY(255) NOT NULL,
    body         LONGBLOB,

    PRIMARY KEY (id, time) -- PK must include all partitioning columns.
)
ROW_FORMAT=COMPRESSED
PARTITION BY RANGE (FLOOR(UNIX_TIMESTAMP(time)))
(
    -- Create a partition that can not be used so that INSERTs fail until the
    -- "real" partitions are created dynamically below. This guarantees that we
    -- never have to modify partitions that already contain data.
    PARTITION temp VALUES LESS THAN (0)
);

CALL alter_partitions('event');

ALTER TABLE event DROP PARTITION IF EXISTS temp;

CREATE EVENT IF NOT EXISTS partition_event
    ON SCHEDULE EVERY 1 MONTH
    ON COMPLETION PRESERVE
    DO CALL alter_partitions('event');
--
-- fact contains mappings of stream and offset to events.
--
-- Every event appended by the client appears on both the named stream it was
-- originally appended to, as well as the ε-stream.
--
CREATE TABLE IF NOT EXISTS fact
(
    store_id BIGINT UNSIGNED NOT NULL,
    stream   VARBINARY(255) NOT NULL,
    offset   BIGINT UNSIGNED NOT NULL,

    -- Note that event's PK is across (event_id, time), due to partitioning.
    event_id BIGINT UNSIGNED NOT NULL,
    time     TIMESTAMP(6) NOT NULL,

    -- There is deliberately no PK defined. Any custom PK would need to include
    -- the time column, since it's used as a partitioning key.
    INDEX (store_id, stream, offset)
)
ROW_FORMAT=COMPRESSED
PARTITION BY RANGE (FLOOR(UNIX_TIMESTAMP(time)))
(
    -- Create a partition that can not be used so that INSERTs fail until the
    -- "real" partitions are created dynamically below. This guarantees that we
    -- never have to modify partitions that already contain data.
    PARTITION temp VALUES LESS THAN (0)
);

CALL alter_partitions('fact');

ALTER TABLE fact DROP PARTITION IF EXISTS temp;

CREATE EVENT IF NOT EXISTS partition_fact
    ON SCHEDULE EVERY 1 MONTH
    ON COMPLETION PRESERVE
    DO CALL alter_partitions('fact');
--
-- store is a mapping of name to event store ID.
--
CREATE TABLE IF NOT EXISTS store
(
    id   BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    name VARBINARY(255) NOT NULL UNIQUE
)
ROW_FORMAT=COMPRESSED;
--
-- stream is the authoratative source for the next offset of each stream. Its
-- rows are locked to ensure consistency of concurrent append operations.
--
-- This table is necessary to allow the 'fact' and 'event' tables to be
-- partitioned and truncated.
--
-- Note also that stream names and offsets are de-normalized, they appear in the
-- 'fact' table, rather than having a relation to this table. This means that
-- 'stream' does not need to be replicated/retained on read-only replicas.
--
CREATE TABLE IF NOT EXISTS stream
(
    store_id BIGINT UNSIGNED NOT NULL,
    name     VARBINARY(255) NOT NULL,
    next     BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY (store_id, name)
)
ROW_FORMAT=COMPRESSED;
--
-- human_view is a human-readable, de-duplicated, chronological report of facts,
-- excluding those on the ε-stream.
--
CREATE
ALGORITHM = MERGE
SQL SECURITY DEFINER
VIEW IF NOT EXISTS human_view AS
    SELECT
        o.name AS store,
        f.time,
        f.stream,
        f.offset,
        e.event_type,
        e.content_type,
        e.body
    FROM store AS o
    INNER JOIN fact AS f
        ON f.store_id = o.id
    INNER JOIN event AS e
        ON e.id = f.event_id
        AND e.time = f.time
    WHERE f.stream != ""
    ORDER BY o.name, e.time, f.stream, f.offset;
`
