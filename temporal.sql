BEGIN;

CREATE SCHEMA IF NOT EXISTS temporal;
COMMENT ON SCHEMA temporal IS 'Upgrade any table with state tracking.';
SET LOCAL search_path TO temporal, public;

CREATE TABLE state (
  txid      bigint NOT NULL DEFAULT txid_current(),
  t         timestamptz NOT NULL DEFAULT now(),
  CHECK (FALSE) NO INHERIT
);
COMMENT ON TABLE state IS
 'Parent of all state tables. (Table inheritances is used to make it easy to '
 'find those tables which are, in fact, state tables.)';

CREATE INDEX "state/txid" ON state (txid);
CREATE INDEX "state/t" ON state (t);

CREATE FUNCTION setup(tab regclass,
                      state_schema name DEFAULT NULL,
                      state_tab name DEFAULT NULL)
RETURNS void AS $$
BEGIN
  EXECUTE temporal.codegen(tab, state_schema, state_tab);
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION setup(regclass, name, name) IS
 'Configures triggers and a state table top provide row versioning.';

CREATE FUNCTION codegen(tab regclass,
                        state_schema name DEFAULT NULL,
                        state_tab name DEFAULT NULL)
RETURNS text AS $code$
DECLARE
  entity_type text;
  entity_pk   text;
  fullname    text;
  code        text := '';
BEGIN
  state_tab := COALESCE(state_tab, meta.tablename(tab)||'/state');
  state_schema := COALESCE(state_schema, meta.schemaname(tab));
  fullname := format('%I.%I', state_schema, state_tab);
  IF meta.schemaname(tab) = state_schema AND
     meta.tablename(tab) = state_tab THEN
    RAISE EXCEPTION 'It looks like we''re trying to create an state table '
                    'with the same name and schema as the base table.';
  END IF;
  code := code || $$
    CREATE TABLE $$||fullname||$$ (
      LIKE temporal.state INCLUDING INDEXES INCLUDING DEFAULTS,
      new $$||tab||$$,
      old $$||tab||$$
    ) INHERITS (temporal.state);
    CREATE FUNCTION temporal.save($$||tab||$$, $$||tab||$$)
    RETURNS void AS $f$
      INSERT INTO $$||fullname||$$ (new, old) VALUES ($1, $2)
    $f$ LANGUAGE sql;
    CREATE TRIGGER temporal AFTER INSERT OR UPDATE OR DELETE
        ON $$||tab||$$
       FOR EACH ROW EXECUTE PROCEDURE temporal.save();
  $$;
  --- Clean up all the whitespace in the generated SQL.
  code := regexp_replace(code, '\n[ ]*$', '', 'g');
  code := regexp_replace(code, '^    ',   '', 'gn');
  RETURN code;
END
$code$ LANGUAGE plpgsql;

CREATE FUNCTION save() RETURNS trigger AS $$
BEGIN
  --- Dispatches to appropriate save function based on row type.
  CASE TG_OP
  WHEN 'INSERT' THEN PERFORM temporal.save(NEW, NULL);
  WHEN 'UPDATE' THEN PERFORM temporal.save(NEW, OLD);
  WHEN 'DELETE' THEN PERFORM temporal.save(NULL, OLD);
  END CASE;
  RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE VIEW registered AS
SELECT typrelid::regclass AS data,
       pg_class.oid::regclass AS states
  FROM pg_class
  JOIN pg_inherits ON (pg_class.oid = inhrelid)
  JOIN pg_attribute ON (pg_class.oid = attrelid)
  JOIN pg_type ON (pg_type.oid = atttypid)
 WHERE inhparent = 'temporal.state'::regclass AND attname = 'new';

END;
