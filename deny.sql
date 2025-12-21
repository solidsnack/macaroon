BEGIN;

SET LOCAL client_min_messages TO error;
DROP SCHEMA IF EXISTS deny CASCADE;
CREATE SCHEMA deny;
COMMENT ON SCHEMA deny IS 'Triggers to disable certain operations.';
SET LOCAL search_path TO deny;  -- All functions will be created in this schema

CREATE FUNCTION deny_updates(tab regclass) RETURNS void AS $code$
BEGIN
  EXECUTE $$
    CREATE TRIGGER deny_updates BEFORE UPDATE
        ON $$||tab||$$
       FOR EACH STATEMENT EXECUTE PROCEDURE deny.deny_updates();
  $$;
END
$code$ LANGUAGE plpgsql;

CREATE FUNCTION deny_updates() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'No updates are allowed on this table.';
  END IF;
  RETURN NULL;
END
$$ LANGUAGE plpgsql STRICT;

CREATE FUNCTION deny_pk_updates(tab regclass) RETURNS void AS $code$
DECLARE
  trigger_name text;
  expr         text;
BEGIN
  trigger_name := format('deny_pk_updates/%I.%I',
                         meta.schemaname(tab),
                         meta.tablename(tab));
  SELECT string_agg(format('OLD.%I = NEW.%I', col, col), ' AND ')
    INTO STRICT expr
    FROM unnest(meta.pk(tab)) AS col;
  EXECUTE $$
    CREATE FUNCTION deny."$$||trigger_name||$$"()
    RETURNS trigger AS $f$
    BEGIN
      IF NOT ($$||expr||$$) THEN
        RAISE EXCEPTION 'Primary key updates are not allowed on this table.';
      END IF;
    END
    $f$ LANGUAGE plpgsql;
    CREATE TRIGGER deny_updates BEFORE UPDATE ON $$||tab||$$
    FOR EACH STATEMENT EXECUTE PROCEDURE deny."$$||trigger_name||$$"();
  $$;
END
$code$ LANGUAGE plpgsql STRICT;

END;

