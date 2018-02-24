--
-- event contains application-defined event data.
--
CREATE TABLE IF NOT EXISTS event(
   id           BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
   event_type   VARBINARY(255) NOT NULL,
   content_type VARBINARY(255) NOT NULL,
   body         LONGBLOB,
   time         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

   INDEX (event_type)
) ROW_FORMAT=COMPRESSED;
