--
-- fact contains mappings of stream and offset to events.
--
-- Every event appended by the client appears on both the named stream it was
-- originally appended to, as well as the ε-stream.
--
CREATE TABLE IF NOT EXISTS fact
(
    id       BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    store_id BIGINT UNSIGNED NOT NULL,
    stream   VARBINARY(255) NOT NULL,
    offset   BIGINT UNSIGNED NOT NULL,
    event_id BIGINT UNSIGNED NOT NULL,
    time     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

    INDEX (store_id, stream, offset),
    INDEX archive (time, store_id)
)
ROW_FORMAT=COMPRESSED;
