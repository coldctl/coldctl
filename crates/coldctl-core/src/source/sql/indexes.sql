SELECT i.indrelid, c.relname::text, am.amname::text,
       i.indisunique, i.indisprimary, i.indisvalid,
       ARRAY(SELECT COALESCE(a.attname::text, '<expression>')
             FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
             LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
             WHERE k.position <= i.indnkeyatts ORDER BY k.position) AS columns,
       ARRAY(SELECT a.attname::text
             FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
             JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
             WHERE k.position > i.indnkeyatts ORDER BY k.position) AS included_columns,
       i.indexprs IS NOT NULL AS has_expressions, i.indpred IS NOT NULL AS partial
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE i.indrelid = ANY($1)
ORDER BY i.indrelid, c.relname
