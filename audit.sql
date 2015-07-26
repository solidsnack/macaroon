BEGIN;

CREATE SCHEMA IF NOT EXISTS audit;
COMMENT ON SCHEMA audit IS
 'Track users and applications performing database operations.';
SET LOCAL search_path TO audit, public;

CREATE TYPE op AS ENUM ('+', '~', '-');

CREATE TABLE event (
  txid      bigint NOT NULL DEFAULT txid_current(),
  op        op NOT NULL,
  tab       regclass NOT NULL,
  t         timestamptz NOT NULL DEFAULT now(),
  who       text NOT NULL DEFAULT session_user,
  app       text NOT NULL DEFAULT application_name(),
  pid       integer NOT NULL DEFAULT pg_backend_pid(),
  PRIMARY KEY (txid, op, tab),
  CHECK (FALSE) NO INHERIT
);
COMMENT ON TABLE event IS
 'Distilled event logs -- the who, what, when and where of an event. Queries '
 'to this table return meta information for all INSERTs, UPDATEs and DELETEs '
 'to tracked tables. When this table is dropped, it takes all the '
 'log tables and logging rules with it, as well.';

CREATE INDEX "event/txid" ON event (txid);
CREATE INDEX "event/t" ON event (t);
CREATE INDEX "event/op" ON event (op);
CREATE INDEX "event/tab" ON event (tab);
CREATE INDEX "event/who" ON event (who);
CREATE INDEX "event/app" ON event (app);
CREATE INDEX "event/pid" ON event (app);

CREATE FUNCTION audit(tab regclass,
                      event_schema name DEFAULT NULL,
                      event_tab name DEFAULT NULL)
RETURNS regclass AS $$
BEGIN
  EXECUTE audit.codegen(tab, event_schema, event_tab);
  RETURN (SELECT events FROM audit.audited WHERE audit.audited.audited = tab);
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION audit(regclass, name, name) IS
 'Triggers to log user, application, pid and time of every INSERT, UPDATE and '
 'DELETE, but not logging the row contents. Every change is associated with a '
 'transaction ID.';

CREATE FUNCTION codegen(tab regclass,
                        event_schema name DEFAULT NULL,
                        event_tab name DEFAULT NULL)
RETURNS text AS $code$
DECLARE
  entity_type text;
  entity_pk   text;
  fullname    text;
  code        text := '';
BEGIN
  event_schema := COALESCE(event_schema, meta.schemaname(tab));
  IF event_schema = meta.schemaname(tab) THEN
    event_tab := COALESCE(event_tab, meta.tablename(tab)||'/event');
  ELSE
    event_tab := COALESCE(event_tab, meta.tablename(tab));
  END IF;
  fullname := format('%I.%I', event_schema, event_tab);
  IF meta.schemaname(tab) = event_schema AND
     meta.tablename(tab) = event_tab THEN
    RAISE EXCEPTION 'It looks like we''re trying to create an event table '
                    'with the same name and schema as the base table.';
  END IF;
  code := code || $$
    CREATE SCHEMA IF NOT EXISTS $$||quote_ident(event_schema)||$$;
    CREATE TABLE $$||fullname||$$ (
      LIKE audit.event INCLUDING INDEXES INCLUDING DEFAULTS,
      CHECK (tab = $$||tab::oid||$$::regclass)
    ) INHERITS (audit.event);
    --- It's for each row so that DELETEs or UPDATEs which don't select any
    --- rows do not get recorded.
    CREATE TRIGGER audit AFTER INSERT OR UPDATE OR DELETE
        ON $$||tab||$$
       FOR EACH ROW EXECUTE PROCEDURE
         audit.save($$||quote_literal(fullname)||$$);
  $$;
  --- Clean up all the whitespace in the generated SQL.
  code := regexp_replace(code, '\n[ ]*$', '', 'g');
  code := regexp_replace(code, '^    ',   '', 'gn');
  RETURN code;
END
$code$ LANGUAGE plpgsql;

CREATE FUNCTION save() RETURNS trigger AS $$
DECLARE
  op        audit.op;
  event_tab regclass;
BEGIN
  CASE TG_OP
  WHEN 'INSERT' THEN op := '+';
  WHEN 'UPDATE' THEN op := '~';
  WHEN 'DELETE' THEN op := '-';
  END CASE;
  event_tab := TG_ARGV[0]::regclass;
  EXECUTE 'INSERT INTO '||event_tab||' (op, tab) VALUES ($1, $2)'
    USING op, TG_RELID;
  RETURN NULL;
EXCEPTION WHEN unique_violation THEN
  --- Do nothing. No point in recording the same transaction ID, user, and
  --- table for every single affected row.
  RETURN NULL;
END
$$ LANGUAGE plpgsql STRICT;

CREATE VIEW audited AS
SELECT tgrelid::regclass AS audited,
       trim('\x00' from tgargs::text)::regclass AS events
  FROM pg_trigger WHERE tgfoid = 'audit.save'::regproc;

END;
