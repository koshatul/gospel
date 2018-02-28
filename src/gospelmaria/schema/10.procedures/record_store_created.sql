--
-- record_store_created records a fact to the ε-stream about a new store being
-- created.
--
-- This function is an implementation detail and should not be called by clients.
--
CREATE PROCEDURE IF NOT EXISTS record_store_created
(
    p_now      TIMESTAMP(6),
    p_store_id BIGINT UNSIGNED,
    p_store    VARBINARY(255)
)
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    CALL record_epsilon(
        p_now,
        p_store_id,
        store_event(
            p_now,
            p_store_id,
            "$store.created",
            "application/vnd.gospel.store.created.v1",
            p_store
        )
    );
END;
