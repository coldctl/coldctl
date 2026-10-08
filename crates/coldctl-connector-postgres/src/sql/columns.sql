SELECT a.attrelid, a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod),
       NOT (a.attnotnull OR t.typnotnull) AS nullable,
       COALESCE(NULLIF(t.typbasetype, 0), t.oid) IN
         ('date'::regtype, 'timestamp'::regtype, 'timestamptz'::regtype) AS time_candidate
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attrelid, a.attnum
