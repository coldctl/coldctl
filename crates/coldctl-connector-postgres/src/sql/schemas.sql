SELECT n.nspname::text
FROM pg_catalog.pg_namespace n
WHERE left(n.nspname, 3) <> 'pg_' AND n.nspname <> 'information_schema'
  AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
ORDER BY n.nspname
