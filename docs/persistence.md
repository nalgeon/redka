# Persistence

Redka stores data in a SQLite database using the following tables:

```
rkey
---
id       integer primary key
key      text not null
type     integer not null    -- 1 string, 2 list, 3 set, 4 hash, 5 sorted set
version  integer not null    -- incremented when the key value is updated
etime    integer             -- expiration timestamp in unix milliseconds
mtime    integer not null    -- modification timestamp in unix milliseconds
len      integer             -- number of child elements

rstring
---
kid      integer not null    -- FK -> rkey.id
value    blob not null

rlist
---
kid      integer not null    -- FK -> rkey.id
pos      real not null       -- is used for ordering, but is not an index
elem     blob not null

rset
---
kid      integer not null    -- FK -> rkey.id
elem     blob not null

rhash
---
kid      integer not null    -- FK -> rkey.id
field    text not null
value    blob not null

rzset
---
kid      integer not null    -- FK -> rkey.id
elem     blob not null
score    real not null
```

To access the data with SQL, use views instead of tables:

```sql
select * from vstring;
```

```
┌─────┬──────┬───────┬───────┬─────────────────────┐
│ kid │ key  │ value │ etime │        mtime        │
├─────┼──────┼───────┼───────┼─────────────────────┤
│ 1   │ name │ alice │       │ 2024-04-03 16:58:14 │
│ 2   │ age  │ 50    │       │ 2024-04-03 16:34:52 │
└─────┴──────┴───────┴───────┴─────────────────────┘
```

`etime` and `mtime` are in UTC.

There is a separate view for every data type:

```
vkey  vstring  vlist  vset  vhash  vzset
```
