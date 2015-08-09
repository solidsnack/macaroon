BEGIN;

CREATE SCHEMA IF NOT EXISTS notifiable;
COMMENT ON SCHEMA notifiable IS
 'Notifications on INSERT, UPDATE and DELETE.';
SET LOCAL search_path TO notifiable, public;

CREATE OR REPLACE FUNCTION setup(tab regclass, channel text DEFAULT NULL)
RETURNS void AS $$
BEGIN
  EXECUTE notifiable.codegen(tab, channel);
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION setup(regclass, text) IS
 'Sets up RULEs such that every INSERT, UPDATE and DELETE to a table will '
 'result in a notification on the chosen channel. The notification payload is '
 'the transaction ID (to allow correlation with audit logs).';

CREATE OR REPLACE FUNCTION codegen(tab regclass, channel text DEFAULT NULL)
RETURNS text AS $code$
BEGIN
  channel := COALESCE(channel, tab::text);
  RETURN $$
    CREATE TRIGGER notifier AFTER INSERT OR UPDATE OR DELETE
        ON $$||tab||$$ FOR EACH ROW EXECUTE PROCEDURE
        notifiable.notifier($$||quote_literal(channel)||$$);
  $$;
END
$code$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION notifier() RETURNS trigger AS $$
DECLARE
  message text;
BEGIN
  CASE TG_OP
  WHEN 'INSERT' THEN message := 'op:+ txid:'||txid_current();
  WHEN 'UPDATE' THEN message := 'op:~ txid:'||txid_current();
  WHEN 'DELETE' THEN message := 'op:- txid:'||txid_current();
  END CASE;
  PERFORM pg_notify(TG_ARGV[0], message);
  RETURN NULL;
END
$$ LANGUAGE plpgsql;

END;
