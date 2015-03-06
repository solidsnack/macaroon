
CREATE FUNCTION pk(tab regclass)
RETURNS TABLE (col name, typ regtype) AS $$
  SELECT (tab||'.'||attname)::name, atttypid::regtype
    FROM pg_attribute JOIN pg_index ON (attnum = ANY (indkey))
   WHERE attrelid = tab AND attnum > 0 AND indrelid = tab AND indisprimary
   ORDER BY attnum
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION cols(tab regclass)
RETURNS TABLE (col name, typ regtype) AS $$
  SELECT (tab||'.'||attname)::name, atttypid::regtype
    FROM pg_attribute
   WHERE attrelid = tab AND attnum > 0
   ORDER BY attnum
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION fk(tab regclass)
RETURNS TABLE (col name, typ regtype) AS $$
  --- TODO: zip(conkey, confkey) and get a schema qualified name for confkey
  SELECT (tab||'.'||attname)::name, atttypid::regtype
    FROM pg_attribute JOIN pg_constraint ON (attnum = ANY (conkey))
   WHERE attrelid = tab AND attnum > 0 AND conrelid = tab
   ORDER BY attnum
$$ LANGUAGE sql STABLE STRICT;
