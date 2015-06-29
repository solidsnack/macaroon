BEGIN;

CREATE SCHEMA IF NOT EXISTS notifiable;
COMMENT ON SCHEMA notifiable IS
 'Notifications on INSERT, UPDATE and DELETE.';
SET LOCAL search_path TO notifiable, public;

CREATE OR REPLACE FUNCTION setup(tab regclass, channel text)
RETURNS void AS $$
BEGIN
  EXECUTE codegen(tab, channel);
END
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION setup(regclass, text) IS
 'Sets up RULEs such that every INSERT, UPDATE and DELETE to a table will '
 'result in a notification on the chosen channel. The notification payload is '
 'one of `+`, `~` or `-` depending on whether the action is an INSERT, UPDATE '
 'or DELETE.';

CREATE OR REPLACE FUNCTION codegen(tab regclass, channel text)
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

END;
