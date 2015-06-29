BEGIN;

CREATE SCHEMA IF NOT EXISTS temporal;
COMMENT ON SCHEMA temporal IS
 'State tracking for tables which already have a column which indicates when '
 'a row was last changed. Updates to primary key columns are not tracked in '
 'any special way (beware if using a natural primary key).';
SET LOCAL search_path TO temporal, public;

CREATE OR REPLACE FUNCTION setup(tab INOUT regclass,
                                 past_schema name DEFAULT NULL,
                                 past_tab name DEFAULT NULL,
                                 past OUT regclass,
                                 notifies INOUT text DEFAULT NULL,
                                 time_column name DEFAULT 't') AS $$
DECLARE
  ddl       text[];
  statement text;
  past_name text;
BEGIN
  SELECT _.ddl, _.past INTO STRICT ddl, past_name
    FROM temporal.codegen(tab, past_schema, past_tab, notifies, time_column)
      AS _;
  FOREACH statement IN ARRAY ddl LOOP
    EXECUTE statement;
  END LOOP;
  past := past_name::regclass;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION codegen(tab INOUT regclass,
                                   past_schema name DEFAULT NULL,
                                   past_tab name DEFAULT NULL,
                                   past OUT text,
                                   notifies INOUT text DEFAULT NULL,
                                   time_column name DEFAULT 't',
                                   ddl OUT text[]) AS $code$
DECLARE
  code text[] := '{}';
  col  name;
  typ  regtype;
  idx  text;
  statement text;
  insert_cols text[] := '{}';
  insert_exprs text[] := '{}';
  timerange_expr text;
BEGIN
  past_schema := COALESCE(past_schema, meta.schemaname(tab));
  past_tab := COALESCE(past_tab, meta.tablename(tab)||'/past');
  past := format('%I.%I', past_schema, past_tab);
  idx := format('%I', past_tab||'/'||time_column);
  IF meta.schemaname(tab) = past_schema AND
     meta.tablename(tab) = past_tab THEN
    RAISE EXCEPTION 'It looks like we''re trying to create a state table '
                    'with the same name and schema as the base table.';
  END IF;
  --- Ensure the schema and table are present.
  code := code || text($$
    CREATE SCHEMA IF NOT EXISTS $$||format('%I', past_schema)||$$;
    CREATE TABLE IF NOT EXISTS $$||past||$$ ();
  $$);
  FOR col, typ IN SELECT _.col, _.typ FROM meta.cols(tab) AS _ LOOP
    IF col = time_column THEN
      CASE typ
      WHEN 'timestamp'::regtype THEN typ := 'tsrange'::regtype;
      WHEN 'timestamptz'::regtype THEN typ := 'tstzrange'::regtype;
      ELSE RAISE EXCEPTION 'Column % is if type %, not a timestamp.', col, typ;
      END CASE;
      timerange_expr := typ || '(COALESCE(OLD.' || quote_ident(col)
                            || ', now()), now())';
      insert_exprs := insert_exprs || timerange_expr;
    ELSE
      insert_exprs := insert_exprs || ('OLD.' || quote_ident(col));
    END IF;
    insert_cols := insert_cols || quote_ident(col);
    code := code || text($$
      --- Idempotent column creation in an exception block.
      DO $do$ BEGIN
        ALTER TABLE $$||past||$$
         ADD COLUMN $$||quote_ident(col)||$$ $$||typ||$$;
      EXCEPTION
        WHEN duplicate_column THEN
          RAISE NOTICE 'Already created column: % %',
                       $$||quote_literal(col)||$$, $$||quote_literal(typ)||$$;
      END $do$;
    $$);
    IF col = time_column THEN
      code := code || text($$
        --- Index for time range colunm, also in an exception block.
        DO $do$ BEGIN
          CREATE INDEX $$||idx||$$ ON $$||past||$$ ($$||col||$$);
        EXCEPTION
          WHEN duplicate_table THEN
            RAISE NOTICE 'Already created index: %',
                         $$||quote_literal(idx)||$$;
        END $do$;
      $$);
    END IF;
  END LOOP;
  code := code || text($$
    --- Note that this is not a trigger. Calling the input row OLD
    --- is a nod to convention.
    CREATE OR REPLACE FUNCTION temporal.remember(OLD $$||tab||$$)
    RETURNS void AS $f$
      INSERT INTO $$||past||$$ ($$||array_to_string(insert_cols, ', ')||$$)
           VALUES ($$||array_to_string(insert_exprs, ', ')||$$);
    $f$ LANGUAGE sql;
    DO $do$ BEGIN
      CREATE TRIGGER remember AFTER UPDATE OR DELETE
          ON $$||tab||$$
         FOR EACH ROW EXECUTE PROCEDURE temporal.remember();
    EXCEPTION
       WHEN duplicate_object
       THEN RAISE NOTICE 'Already created trigger remember on %',
            $$||quote_literal(tab)||$$;
    END $do$;
  $$);
  IF notifies IS NOT NULL THEN
    code := code || temporal.notifiable(tab, notifies);
  END IF;
  FOREACH statement IN ARRAY code LOOP
    --- Clean up all the whitespace in the generated SQL.
    statement := regexp_replace(statement, '\n[ ]*$', '', 'g');
    statement := regexp_replace(statement, '^    ',   '', 'gn');
    ddl := ddl || statement;
  END LOOP;
END
$code$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION notifiable(tab regclass, channel text)
RETURNS text AS $code$
BEGIN
  RETURN $$
    CREATE OR REPLACE RULE notifiable_ins AS ON INSERT TO $$||tab||$$
        DO ALSO NOTIFY $$||quote_ident(channel)||$$, '+';
    CREATE OR REPLACE RULE notifiable_upd AS ON UPDATE TO $$||tab||$$
        DO ALSO NOTIFY $$||quote_ident(channel)||$$, '~';
    CREATE OR REPLACE RULE notifiable_del AS ON DELETE TO $$||tab||$$
        DO ALSO NOTIFY $$||quote_ident(channel)||$$, '-';
  $$;
END
$code$ LANGUAGE plpgsql;

--- This trigger relies on functions of the same name -- remember(...) -- that
--- do the actual updates. Overloads ensure the right variant is called.
CREATE OR REPLACE FUNCTION remember() RETURNS trigger AS $$
BEGIN
  CASE TG_OP
  WHEN 'UPDATE' THEN
    PERFORM temporal.remember(OLD);
    RETURN NEW;
  WHEN 'DELETE' THEN
    PERFORM temporal.remember(OLD);
    RETURN OLD;
  END CASE;
END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION quote_type(typ regtype) RETURNS text AS $$
  SELECT quote_ident(typ::name)
$$ LANGUAGE sql IMMUTABLE STRICT;

END;
