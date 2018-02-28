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
    event_id BIGINT UNSIGNED NOT NULL,
    time     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

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
