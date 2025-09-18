CREATE TEMP TABLE IF NOT EXISTS t_counts(
  table_schema text,
  table_name   text,
  row_count    bigint
) ON COMMIT PRESERVE ROWS;

TRUNCATE t_counts;

DO $$
DECLARE r record; v bigint;
BEGIN
  FOR r IN
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_schema = 'public'
    ORDER BY table_schema, table_name
  LOOP
    EXECUTE format('SELECT count(*) FROM %I.%I', r.table_schema, r.table_name) INTO v;
    INSERT INTO t_counts VALUES (r.table_schema, r.table_name, v);
  END LOOP;
END $$;

SELECT * FROM t_counts
ORDER BY row_count DESC, table_schema, table_name;

SELECT sum(row_count) AS total_rows FROM t_counts;
