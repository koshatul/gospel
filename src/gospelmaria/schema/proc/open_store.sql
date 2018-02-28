--
-- open_store returns the ID of the store named p_store, creating it if it does
-- not already exist.
--
CREATE FUNCTION IF NOT EXISTS open_store
(
    p_store VARBINARY(255)
)
RETURNS BIGINT UNSIGNED
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_store_id BIGINT UNSIGNED;
    DECLARE v_exists BOOLEAN DEFAULT FALSE;

    DECLARE CONTINUE HANDLER FOR 1062 SET v_exists = TRUE; -- 1062 = duplicate key

    INSERT INTO store SET
        name = p_store;

    SELECT id
        INTO v_store_id
        FROM store
    WHERE
        name = p_store;

    -- Create the ε-stream and record an event about store creation.
    IF v_exists = FALSE THEN
        INSERT INTO stream SET
            store_id = v_store_id,
            name     = "",
            next     = 0;

        CALL record_store_created(v_store_id, p_store);
    END IF;

    RETURN v_store_id;
END;
