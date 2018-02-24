--
-- record_stream_created records a fact to the ε-stream about a new named stream
-- being created.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE PROCEDURE IF NOT EXISTS record_stream_created
(
    p_store_id BIGINT UNSIGNED,
    p_stream   VARBINARY(255)
)
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    SELECT record_fact(
        p_store_id,
        "",
        store_event(
            "$stream.created",
            "application/vnd.streakdb.stream.created.v1",
            p_stream
        )
    ) INTO @_;
END;
