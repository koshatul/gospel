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
