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
