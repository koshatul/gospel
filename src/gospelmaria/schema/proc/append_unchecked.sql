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
    DECLARE v_event_id BIGINT UNSIGNED;
    DECLARE v_offset BIGINT UNSIGNED;

    IF p_stream = "" THEN
        SIGNAL SQLSTATE '45000' SET
            MESSAGE_TEXT='cannot append directly to the ε-stream';
    END IF;

    -- Record the event first, defering the locking of the stream table as late
    -- as possible.
    SET v_event_id = store_event(
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
        CALL record_stream_created(p_store_id, p_stream);
    END IF;

    -- Record our fact on the ε-stream.
    CALL record_epsilon(p_store_id, v_event_id);

    -- Record our fact on the named stream.
    INSERT INTO fact SET
        store_id = p_store_id,
        stream   = p_stream,
        offset   = v_offset,
        event_id = v_event_id;

    RETURN v_offset;
END;
