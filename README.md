
Many tricky table configurations -- for example, logged tables and partitioned
tables -- would seem to be amenable to mechanical derivation. The SQL standard
provides for fairly rich introspection capabilities -- on par with any object
oriented language -- so it stands to reason that we should be able to use
metaprogramming to derive advanced table configurations mechanically.

The `meta` schema in `meta.sql` should be loaded before loading the others. To
load all the schemas together, run `\i macaroon.psql` at the `psql` prompt.

Creating Tables for Auditing & Versioning
-------------------------------------------

Imagine that your application has tables in the `app` namespace; and you'd like
to log past row versions and metadata about changes to the `state` and `events`
schemas, respectively. You can idempotently configure both audit and version
stracking by `SELECT`ing tables in the `app` namespace that are not already
tracked and passing them to the setup functions:

```sql
SELECT tab,
       temporal.temporal(tab, 'state'),
       audit.audit(tab, 'events')
  FROM pg_tables,
       LATERAL (SELECT (schemaname||'.'||tablename)::regclass AS tab)
            AS casted_to_regclass
 WHERE schemaname = 'app'
   AND tab NOT IN (SELECT logged FROM temporal.logged
                    UNION
                   SELECT audited FROM audit.audited);

      tab      │    temporal     │      audit
───────────────┼─────────────────┼──────────────────
 app.user_info │ state.user_info │ events.user_info
 app.telephone │ state.telephone │ events.telephone
 app.cpu       │ state.cpu       │ events.cpu
 ...           │ ...             │ ...
```


Using The Auditing & Versioning Tables
--------------------------------------

The audit and temporal tables for each table that is tracked can be joined on
the `txid` column to see all the actions that took place during a particular
transaction.


A Note About Migrations
-----------------------

The audit tables are indifferent to migrations -- they do not store any row
data.

The temporal tables store `JSONB`, so they're also indifferent to migrations.

