--
-- human_view is a human-readable, de-duplicated, chronological report of facts,
-- excluding those on the ε-stream.
--
CREATE
ALGORITHM = MERGE
SQL SECURITY DEFINER
VIEW IF NOT EXISTS human_view AS
    SELECT
        o.name AS store,
        f.time,
        f.stream,
        f.offset,
        e.event_type,
        e.content_type,
        e.body
    FROM store AS o
    INNER JOIN fact AS f
        ON f.store_id = o.id
    INNER JOIN event AS e
        ON e.id = f.event_id
        AND e.time = f.time
    WHERE f.stream != ""
    ORDER BY o.name, e.time, f.stream, f.offset;
