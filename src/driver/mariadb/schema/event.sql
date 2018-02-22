--
-- event stores application-defined event data.
--
CREATE TABLE IF NOT EXISTS event(
   id           BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
   store        VARCHAR(255) NOT NULL,
   event_type   VARCHAR(255) NOT NULL,
   content_type VARCHAR(255) NOT NULL,
   body         LONGBLOB,
   time         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

   INDEX (event_type)
) ROW_FORMAT=COMPRESSED;
