--
-- record_fact unconditionally records a fact and returns its offset without
-- recording any information about new streams.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE FUNCTION IF NOT EXISTS record_fact
(
    p_store    VARBINARY(255),
    p_stream   VARBINARY(255),
    p_event_id BIGINT UNSIGNED
)
RETURNS BIGINT UNSIGNED
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_offset BIGINT UNSIGNED;

    SELECT offset + 1
        INTO v_offset
        FROM fact
    WHERE store = p_store
        AND stream = p_stream
        ORDER BY offset DESC
        LIMIT 1
        FOR UPDATE;

    IF v_offset IS NULL THEN
        SET v_offset = 0;
    END IF;

    INSERT INTO fact SET
        store    = p_store,
        stream   = p_stream,
        offset   = v_offset,
        event_id = p_event_id;

    RETURN v_offset;
END;
