
CREATE TYPE column_info
         AS (num smallint, name name, datatype regtype, pk boolean);

CREATE FUNCTION column_info(tab regclass) RETURNS SETOF column_info AS $$
  WITH pk_indexes AS
  ( SELECT unnest(indkey) AS column_number FROM pg_index
     WHERE indrelid = tab AND indisprimary )
  SELECT attnum, attname, atttypid::regtype,
         EXISTS (SELECT * FROM pk_indexes WHERE column_number = attnum) 
    FROM pg_attribute
   WHERE attrelid = tab AND attnum > 0
   ORDER BY attnum
$$ LANGUAGE sql STABLE STRICT
   SET search_path FROM CURRENT;

CREATE FUNCTION true_ts_column(tab regclass) RETURNS column_info AS $$
  SELECT * FROM column_info(tab)
   WHERE datatype = 'timestamptz'::regtype AND NOT pk
   ORDER BY num LIMIT 1
$$ LANGUAGE sql STABLE STRICT
   SET search_path FROM CURRENT;


