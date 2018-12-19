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


CREATE TYPE mode AS ENUM ('only_old', 'only_new', 'old_and_new');


CREATE FUNCTION temporal(tab regclass,
                         state_schema name DEFAULT NULL,
                         state_tab name DEFAULT NULL,
                         mode mode DEFAULT 'only_new')
RETURNS regclass AS $$
DECLARE
  txt text;
BEGIN
  FOR txt IN SELECT * FROM temporal.codegen(tab, state_schema, state_tab, mode)
  LOOP
    EXECUTE txt;
  END LOOP;
  RETURN (SELECT states FROM temporal.logged
           WHERE temporal.logged.logged = tab);
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION temporal(regclass, name, name, mode) IS
 'Configures triggers and a state table to provide row versioning.';


CREATE FUNCTION codegen(tab regclass,
                        state_schema name DEFAULT NULL,
                        state_tab name DEFAULT NULL,
                        mode mode DEFAULT 'only_new')
RETURNS TABLE (statement text) AS $code$
DECLARE
  fullname    text;
  code        text[] := ARRAY[]::text[];
  tabledef    text[] := ARRAY[]::text[];
  fielddef    text[] := ARRAY[]::text[];
  valuedef    text[] := ARRAY[]::text[];
  eventdef    text[] := ARRAY[]::text[];
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

  IF mode NOT IN ('only_old') THEN
    tabledef := tabledef || ARRAY['new_tid tid', 'new jsonb'];
    fielddef := fielddef || ARRAY['new_tid',  'new'];
    valuedef := valuedef || ARRAY['$1', 'row_to_json($2)::jsonb'];
    eventdef := ARRAY['INSERT', 'UPDATE', 'DELETE'];
  END IF;

  IF mode NOT IN ('only_new') THEN
    tabledef := tabledef || ARRAY['old_tid tid', 'old jsonb'];
    fielddef := fielddef || ARRAY['old_tid',  'old'];
    valuedef := valuedef || ARRAY['$3', 'row_to_json($4)::jsonb'];
    eventdef := ARRAY['UPDATE', 'DELETE'];
  END IF;

  code := code || ARRAY[$$
    CREATE SCHEMA IF NOT EXISTS $$||quote_ident(state_schema)||$$;
  $$];

  code := code || ARRAY[$$
    CREATE TABLE $$||fullname||$$ (
      LIKE temporal.state INCLUDING INDEXES INCLUDING DEFAULTS,
      $$||array_to_string(tabledef, ', ')||$$
    ) INHERITS (temporal.state);
  $$];

  code := code || ARRAY[$$
    CREATE FUNCTION temporal.save(tid, $$||tab||$$, tid, $$||tab||$$)
    RETURNS $$||fullname||$$ AS $save$
      INSERT INTO $$||fullname||$$ ($$||array_to_string(fielddef, ', ')||$$)
      VALUES ($$||array_to_string(valuedef, ', ')||$$)
      RETURNING *
    $save$ LANGUAGE sql;
  $$];

  code := code || ARRAY[$$
    CREATE TRIGGER temporal
     AFTER $$||array_to_string(eventdef, ' OR ')||$$ ON $$||tab||$$
       FOR EACH ROW EXECUTE PROCEDURE temporal.save();
  $$];

  FOREACH statement IN ARRAY code LOOP
    --- Clean up all the whitespace in the generated SQL.
    statement := regexp_replace(statement, '^\n', '', 'g');
    statement := regexp_replace(statement, '[ ]*$', '', 'g');
    statement := regexp_replace(statement, '^    ',   '', 'gn');
    RETURN NEXT;
  END LOOP;
END
$code$ LANGUAGE plpgsql;


CREATE FUNCTION save() RETURNS trigger AS $$
BEGIN
  --- Dispatches to appropriate save function based on row type.
  CASE TG_OP
  WHEN 'INSERT' THEN PERFORM temporal.save(NEW.ctid, NEW, NULL, NULL);
  WHEN 'UPDATE' THEN PERFORM temporal.save(NEW.ctid, NEW, OLD.ctid, OLD);
  WHEN 'DELETE' THEN PERFORM temporal.save(NULL, NULL, OLD.ctid, OLD);
  END CASE;
  RETURN NULL;
END
$$ LANGUAGE plpgsql;


CREATE VIEW logged AS
SELECT logged.oid::regclass AS logged,
       states.oid::regclass AS states
  FROM pg_class AS states
  JOIN pg_inherits ON inhrelid = states.oid
  JOIN pg_proc ON prorettype = states.reltype
  JOIN pg_class AS logged ON logged.reltype = proargtypes[1]
 WHERE inhparent = 'temporal.state'::regclass
   AND pronamespace = 'temporal'::regnamespace AND proname = 'save';

END;
