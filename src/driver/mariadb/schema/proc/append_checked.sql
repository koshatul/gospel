--
-- append_checked records a new fact to a named stream and to the ε-stream.
--
-- It returns TRUE if the event is appended, or FALSE if there is a conflict.
-- A conflict occurs when p_offset is not the next unused offset of p_stream.
--
CREATE FUNCTION IF NOT EXISTS append_checked
(
    p_store        VARCHAR(255),
    p_stream       VARCHAR(255),
    p_offset       BIGINT UNSIGNED,
    p_event_type   VARCHAR(255),
    p_content_type VARCHAR(255),
    p_body         LONGBLOB
)
RETURNS BOOLEAN
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_event_id BIGINT UNSIGNED;
    DECLARE v_offset BIGINT UNSIGNED;

    IF p_stream = "" THEN
        SIGNAL SQLSTATE '45000' SET
            MESSAGE_TEXT='Cannot append directly to the ε-stream.';
    END IF;

    SET v_event_id = store_event(
        p_store,
        p_event_type,
        p_content_type,
        p_body
    );

    SET v_offset = record_fact(
        p_store,
        p_stream,
        v_event_id
    );

    IF v_offset != p_offset THEN
        RETURN FALSE;
    END IF;

    IF v_offset = 0 THEN
        CALL record_stream_created(p_store, p_stream);
    END IF;

    SELECT record_fact(
        p_store,
        "",
        v_event_id
    ) INTO @_;

    RETURN TRUE;
END;
