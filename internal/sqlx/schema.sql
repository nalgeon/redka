pragma user_version = 1;

-- ┌───────────────┐
-- │ Keys          │
-- └───────────────┘
-- Types:
-- 1 - string
-- 2 - list
-- 3 - set
-- 4 - hash
-- 5 - zset (sorted set)
create table if not exists
rkey (
    id       integer primary key,
    key      text not null,
    type     integer not null,
    version  integer not null,
    etime    integer,
    mtime    integer not null,
    len      integer
);

create unique index if not exists
rkey_key_idx on rkey (key);

create index if not exists
rkey_etime_idx on rkey (etime)
where etime is not null;

-- ┌───────────────┐
-- │ Strings       │
-- └───────────────┘
create table if not exists
rstring (
    key_id integer not null,
    value  blob not null,

    foreign key (key_id) references rkey (id)
    on delete cascade
);

create unique index if not exists
rstring_pk_idx on rstring (key_id);

create view if not exists
vstring as
select
    rkey.id as key_id, rkey.key, rstring.value,
    datetime(etime/1000, 'unixepoch') as etime,
    datetime(mtime/1000, 'unixepoch') as mtime
from rkey join rstring on rkey.id = rstring.key_id
where rkey.type = 1
    and (rkey.etime is null or rkey.etime > unixepoch('subsec'));

-- ┌───────────────┐
-- │ Lists         │
-- └───────────────┘
create table if not exists
rlist (
    key_id integer not null,
    pos    real not null,
    elem   blob not null,

    foreign key (key_id) references rkey (id)
    on delete cascade
);

create unique index if not exists
rlist_pk_idx on rlist (key_id, pos);

create trigger if not exists
rlist_on_update
before update on rlist
for each row
begin
    update rkey set
        version = version + 1,
        mtime = unixepoch('subsec') * 1000
    where id = old.key_id;
end;

create trigger if not exists
rlist_on_delete
before delete on rlist
for each row
begin
    update rkey set
        version = version + 1,
        mtime = unixepoch('subsec') * 1000,
        len = len - 1
    where id = old.key_id;
end;

create view if not exists
vlist as
select
    rkey.id as key_id, rkey.key,
    row_number() over w as idx, rlist.elem,
    datetime(etime/1000, 'unixepoch') as etime,
    datetime(mtime/1000, 'unixepoch') as mtime
from rkey join rlist on rkey.id = rlist.key_id
where rkey.type = 2
    and (rkey.etime is null or rkey.etime > unixepoch('subsec'))
window w as (partition by key_id order by pos);

-- ┌───────────────┐
-- │ Hashes        │
-- └───────────────┘
create table if not exists
rhash (
    key_id integer not null,
    field text not null,
    value blob not null,

    foreign key (key_id) references rkey (id)
    on delete cascade
);

create unique index if not exists
rhash_pk_idx on rhash (key_id, field);

create trigger if not exists
rhash_on_insert
before insert on rhash
for each row
when (
        select count(*) from rhash
        where key_id = new.key_id and field = new.field
    ) = 0
begin
    update rkey
    set len = len + 1
    where id = new.key_id;
end;

create view if not exists
vhash as
select
    rkey.id as key_id, rkey.key, rhash.field, rhash.value,
    datetime(etime/1000, 'unixepoch') as etime,
    datetime(mtime/1000, 'unixepoch') as mtime
from rkey join rhash on rkey.id = rhash.key_id
where rkey.type = 4
    and (rkey.etime is null or rkey.etime > unixepoch('subsec'));

-- ┌───────────────┐
-- │ Sorted sets   │
-- └───────────────┘
create table if not exists
rzset (
    key_id integer not null,
    elem   blob not null,
    score  real not null,

    foreign key (key_id) references rkey (id)
    on delete cascade
);

create unique index if not exists
rzset_pk_idx on rzset (key_id, elem);

create index if not exists
rzset_score_idx on rzset (key_id, score, elem);

create trigger if not exists
rzset_on_insert
before insert on rzset
for each row
when (
        select count(*) from rzset
        where key_id = new.key_id and elem = new.elem
    ) = 0
begin
    update rkey
    set len = len + 1
    where id = new.key_id;
end;

create view if not exists
vzset as
select
    rkey.id as key_id, rkey.key, rzset.elem, rzset.score,
    datetime(etime/1000, 'unixepoch') as etime,
    datetime(mtime/1000, 'unixepoch') as mtime
from rkey join rzset on rkey.id = rzset.key_id
where rkey.type = 5
    and (rkey.etime is null or rkey.etime > unixepoch('subsec'));
