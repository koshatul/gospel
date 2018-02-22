--
-- fact stores stream and offset information about events.
--
-- Every event appended by the client appears on both the named stream it was
-- originally appended to, as well as the ε-stream.
--
CREATE TABLE IF NOT EXISTS fact(
    store    VARCHAR(255) NOT NULL,
    stream   VARCHAR(255) NOT NULL,
    offset   BIGINT UNSIGNED NOT NULL,
    event_id BIGINT UNSIGNED NOT NULL,
    time     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

    PRIMARY KEY (store, stream, offset)
) ROW_FORMAT=COMPRESSED;
