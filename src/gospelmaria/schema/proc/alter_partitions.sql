CREATE PROCEDURE IF NOT EXISTS alter_partitions
(
    p_table VARCHAR(255)
)
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    -- Use UTC explicitly to prevent any ambiguity as to which partition
    -- should contain rows that are inserted near the 1st of each month.
    DECLARE v_now   TIMESTAMP DEFAULT UTC_TIMESTAMP;
    DECLARE v_year  INTEGER DEFAULT YEAR(v_now);
    DECLARE v_month INTEGER DEFAULT MONTH(v_now);

    DECLARE v_threshold_curr BIGINT;
    DECLARE v_threshold_next BIGINT;

    -- @threshold_[curr|next] are the Unix timestamps used in the range
    -- partition for the current month and next month, respectively.
    --
    -- Note that because partitions are defined by their UPPER BOUND, the
    -- thresholds refer to the next month (v_month + 1) and the month after
    -- that (v_month + 2).
    --
    -- We have to use CONVERT_TZ() because UNIX_TIMESTAMP() always assumes the
    -- string it's parsing is in the server/session timezone (even if another
    -- timezone is specified in the string).
    SET v_threshold_curr = UNIX_TIMESTAMP(
        CONVERT_TZ(
            CONCAT(v_year, '-', v_month + 1, '-01 00:00:00'),
            'UTC',
            @@session.time_zone
        )
    );

    SET v_threshold_next = UNIX_TIMESTAMP(
        CONVERT_TZ(
            CONCAT(v_year, '-', v_month + 2, '-01 00:00:00'),
            'UTC',
            @@session.time_zone
        )
    );

    PREPARE st FROM CONCAT(
        'ALTER TABLE ', p_table, '
        ADD PARTITION IF NOT EXISTS (
            PARTITION P_', v_year, '_', LPAD(v_month+0, 2, '0'), ' VALUES LESS THAN (', v_threshold_curr, '),
            PARTITION P_', v_year, '_', LPAD(v_month+1, 2, '0'), ' VALUES LESS THAN (', v_threshold_next, ')
        )'
    );
    EXECUTE st;
    DEALLOCATE PREPARE st;
END;
