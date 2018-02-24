--
-- store_event inserts an event and returns its auto-increment ID.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE FUNCTION IF NOT EXISTS store_event
(
    p_store        VARBINARY(255),
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
        store        = p_store,
        event_type   = p_event_type,
        content_type = p_content_type,
        body         = p_body;

    RETURN LAST_INSERT_ID();
END;
