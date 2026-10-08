-- Physical file sizes and existing statistics only. Never scan customer rows or refresh statistics.
-- Partitioned parents have no aggregate storage here; leaves are reported independently.
SELECT c.oid,
       CASE WHEN c.relkind = 'p' THEN NULL ELSE pg_catalog.pg_total_relation_size(c.oid) END,
       CASE WHEN c.relkind = 'p' THEN NULL ELSE pg_catalog.pg_table_size(c.oid) END,
       CASE WHEN c.relkind = 'p' THEN NULL ELSE pg_catalog.pg_indexes_size(c.oid) END,
       to_char(GREATEST(s.last_analyze, s.last_autoanalyze) AT TIME ZONE 'UTC',
               'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
       s.n_mod_since_analyze
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid
WHERE c.oid = ANY($1)
