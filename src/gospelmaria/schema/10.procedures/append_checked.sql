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
