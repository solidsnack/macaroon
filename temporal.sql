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

CREATE FUNCTION temporal(tab regclass,
                         state_schema name DEFAULT NULL,
                         state_tab name DEFAULT NULL,
                         with_old boolean DEFAULT FALSE)
RETURNS regclass AS $$
BEGIN
  EXECUTE temporal.codegen(tab, state_schema, state_tab, with_old);
  RETURN (SELECT states FROM temporal.logged
           WHERE temporal.logged.logged = tab);
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION temporal(regclass, name, name, boolean) IS
 'Configures triggers and a state table top provide row versioning.';

CREATE FUNCTION codegen(tab regclass,
                        state_schema name DEFAULT NULL,
                        state_tab name DEFAULT NULL,
                        with_old boolean DEFAULT FALSE)
RETURNS text AS $code$
DECLARE
  entity_type text;
  entity_pk   text;
  fullname    text;
  code        text := '';
BEGIN
  state_schema := COALESCE(state_schema, meta.schemaname(tab));
  IF state_schema = meta.schemaname(tab) THEN
    state_tab := COALESCE(state_tab, meta.tablename(tab)||'/state');
  ELSE
    state_tab := COALESCE(state_tab, meta.tablename(tab));
  END IF;
  fullname := format('%I.%I', state_schema, state_tab);
  IF meta.schemaname(tab) = state_schema AND
     meta.tablename(tab) = state_tab THEN
    RAISE EXCEPTION 'It looks like we''re trying to create a state table '
                    'with the same name and schema as the base table.';
  END IF;
  code := code || $$
    CREATE SCHEMA IF NOT EXISTS $$||quote_ident(state_schema)||$$;
  $$;
  IF with_old THEN
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
    $$;
  ELSE
    code := code || $$
      CREATE TABLE $$||fullname||$$ (
        LIKE temporal.state INCLUDING INDEXES INCLUDING DEFAULTS,
        new $$||tab||$$
      ) INHERITS (temporal.state);
      CREATE FUNCTION temporal.save($$||tab||$$, $$||tab||$$)
      RETURNS void AS $f$
        INSERT INTO $$||fullname||$$ (new) VALUES ($1)
      $f$ LANGUAGE sql;
    $$;
  END IF;
  code := code || $$
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

CREATE VIEW logged AS
SELECT typrelid::regclass AS logged,
       pg_class.oid::regclass AS states
  FROM pg_class
  JOIN pg_inherits ON (pg_class.oid = inhrelid)
  JOIN pg_attribute ON (pg_class.oid = attrelid)
  JOIN pg_type ON (pg_type.oid = atttypid)
 WHERE inhparent = 'temporal.state'::regclass AND attname = 'new';

END;
