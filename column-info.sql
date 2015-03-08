BEGIN;

DROP SCHEMA IF EXISTS meta CASCADE;
CREATE SCHEMA meta;
SET LOCAL search_path TO meta;  -- All functions will be created in this schema

CREATE VIEW pk AS
SELECT attrelid::regclass AS tab,
       array_agg(attname)::name[] AS cols
  FROM pg_attribute
  JOIN pg_index ON (attrelid = indrelid AND attnum = ANY (indkey))
 WHERE indisprimary
 GROUP BY attrelid;

CREATE VIEW cols AS
SELECT attrelid::regclass AS tab,
       attname::name AS col,
       atttypid::regtype AS typ
  FROM pg_attribute
 WHERE attnum > 0
 ORDER BY attrelid, attnum;

CREATE VIEW fk AS
SELECT conrelid::regclass AS tab,
       array_agg(self.attname)::name[] AS cols,
       confrelid::regclass AS other,
       array_agg(other.attname)::name[] AS refs
  FROM pg_constraint
  JOIN pg_attribute AS self
    ON (self.attrelid = conrelid AND self.attnum = ANY (conkey))
  JOIN pg_attribute AS other
    ON (other.attrelid = conrelid AND other.attnum = ANY (conkey))
 WHERE confrelid != 0
 GROUP BY conrelid, confrelid;

CREATE FUNCTION ns(tab regclass) RETURNS name AS $$
  SELECT nspname
    FROM pg_class JOIN pg_namespace ON (pg_namespace.oid = relnamespace)
   WHERE pg_class.oid = tab
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION pk(t regclass) RETURNS name[] AS $$
  SELECT cols FROM meta.pk WHERE meta.pk.tab = t;
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION cols(t regclass)
RETURNS TABLE (col name, typ regtype) AS $$
  SELECT col, typ FROM meta.cols WHERE meta.cols.tab = t;
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION fk(t regclass)
RETURNS TABLE (cols name[], other regclass, refs name[]) AS $$
  SELECT cols, other, refs FROM meta.fk WHERE meta.fk.tab = t;
$$ LANGUAGE sql STABLE STRICT;

COMMIT;
