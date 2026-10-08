SELECT c.oid, n.nspname::text, c.relname::text, c.relkind = 'p' AS partitioned,
       CASE WHEN c.reltuples < 0 OR c.relkind = 'p' THEN NULL ELSE c.reltuples::float8 END AS estimated_rows
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND left(n.nspname, 3) <> 'pg_' AND n.nspname <> 'information_schema'
  AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
  AND pg_catalog.has_table_privilege(c.oid, 'SELECT')
ORDER BY n.nspname, c.relname
