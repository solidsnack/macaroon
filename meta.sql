BEGIN;

SET LOCAL client_min_messages TO error;
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
       atttypid::regtype AS typ,
       attnum AS num
  FROM pg_attribute
 WHERE attnum > 0
 ORDER BY attrelid, attnum;

CREATE VIEW fk AS
SELECT conrelid::regclass AS tab,
       names.cols,
       confrelid::regclass AS other,
       names.refs
  FROM pg_constraint,
       LATERAL (SELECT array_agg(cols.attname) AS cols,
                       array_agg(cols.attnum)  AS nums,
                       array_agg(refs.attname) AS refs
                  FROM unnest(conkey, confkey) AS _(col, ref),
                       LATERAL (SELECT * FROM pg_attribute
                                 WHERE attrelid = conrelid AND attnum = col)
                            AS cols,
                       LATERAL (SELECT * FROM pg_attribute
                                 WHERE attrelid = confrelid AND attnum = ref)
                            AS refs)
            AS names
 WHERE confrelid != 0
 ORDER BY (conrelid, names.nums);             -- Returned in column index order

CREATE FUNCTION ns(tab regclass) RETURNS name AS $$
  SELECT nspname
    FROM pg_class JOIN pg_namespace ON (pg_namespace.oid = relnamespace)
   WHERE pg_class.oid = tab
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION pk(t regclass) RETURNS name[] AS $$
  SELECT cols FROM meta.pk WHERE meta.pk.tab = t;
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION cols(t regclass)
RETURNS TABLE (num smallint, col name, typ regtype) AS $$
  SELECT num, col, typ FROM meta.cols WHERE meta.cols.tab = t;
$$ LANGUAGE sql STABLE STRICT;

CREATE FUNCTION fk(t regclass)
RETURNS TABLE (cols name[], other regclass, refs name[]) AS $$
  SELECT cols, other, refs FROM meta.fk WHERE meta.fk.tab = t;
$$ LANGUAGE sql STABLE STRICT;

END;
