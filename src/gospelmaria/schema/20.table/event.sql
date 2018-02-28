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
