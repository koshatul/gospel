--
-- append_unchecked records a new fact to a named stream and to the ε-stream.
--
CREATE FUNCTION IF NOT EXISTS append_unchecked
(
    p_store        VARBINARY(255),
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

    IF v_offset = 0 THEN
        CALL record_stream_created(p_store, p_stream);
    END IF;

    SELECT record_fact(
        p_store,
        "",
        v_event_id
    ) INTO @_;

    RETURN v_offset;
END;
