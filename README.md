

Many tricky table configurations -- for example, logged tables and partitioned
tables -- would seem to be amenable to mechanical derivation. The SQL standard
provides for fairly rich introspection capabilities -- on par with any object
oriented language -- so it stands to reason that we should be able to use
metaprogramming to derive advanced table configurations mechanically.

The `meta` schema in `meta.sql` should be loaded before loading the others.


Temporal Tables
---------------

Simple temporal tables -- not really as rich as those provided for by SQL 2011,
but a start -- can be derived using `temporal.setup` from `temporal.sql`. If
`notifies` is passed in addition to the table name, then notifications on
`INSERT`, `UPDATE` and `DELETE` will be setup.

```sql
CREATE TABLE abc (
  abc SERIAL PRIMARY KEY,
  t timestamptz DEFAULT now(),
  data text
);
--- CREATE TABLE
--- # solidsnack@[local]/~
--- = SELECT * FROM temporal.setup('abc', notifies := 'abc');
---  tab │    past    │ notifies 
--- ─────┼────────────┼──────────
---  abc │ "abc/past" │ abc
--- (1 row)

INSERT INTO abc VALUES (DEFAULT, DEFAULT, 'some text') RETURNING abc;
---  abc
--- ─────
---    1
--- (1 row)
---
--- INSERT 0 1
--- Asynchronous notification "abc" with payload "+" received from server process with PID 76097.

UPDATE abc SET data = 'other text' WHERE abc = 1;
--- UPDATE 1
--- Asynchronous notification "abc" with payload "~" received from server process with PID 76097.

SELECT * FROM "abc/past";
--- ─[ RECORD 1 ]───────────────────────────────────────────────────────────
--- abc  │ 1
--- t    │ ["2015-06-28 00:00:53.268243-07","2015-06-28 00:01:11.899717-07")
--- data │ some text
```

The interaction between SQL macros and migrations is still very much a work in
progress. Adding a column and rerunning the macro works as you would expect it
to; the trigger and underlying table are updated to reflect the new column.
This does not work, however, for column removal or change of type.

One thing to note is that only older data shows up in the table of states.
This is one way that these tables are different from true temporal tables.
Another way they are different is that there is no way to track a change of
primary key.
