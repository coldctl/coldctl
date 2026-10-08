-- Semantic storage migration: legacy cursor values remain SQLite INTEGERs.
-- ObjectId cursors use exactly 12-byte BLOBs in the existing affinity columns.
-- Recording version 10 prevents older agents from interpreting document jobs.
-- No table rewrite is needed: these tables are deliberately not STRICT.
SELECT 1;
