--
-- record_epsilon unconditionally records a fact to the ε-stream.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE PROCEDURE IF NOT EXISTS record_epsilon
(
    p_store_id BIGINT UNSIGNED,
    p_event_id BIGINT UNSIGNED
)
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_offset BIGINT UNSIGNED;

    SELECT next
        INTO v_offset
        FROM stream
    WHERE store_id = p_store_id
        AND name = ""
        FOR UPDATE;

    IF v_offset IS NULL THEN
        SIGNAL SQLSTATE '45000' SET
            MESSAGE_TEXT='ε-stream does not exist';
    END IF;

    INSERT INTO fact SET
        store_id = p_store_id,
        stream   = "",
        offset   = v_offset,
        event_id = p_event_id;

    UPDATE stream SET
        next = v_offset + 1
    WHERE store_id = p_store_id
        AND name = "";
END;
