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
) strict;

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
    kid    integer not null,
    value  blob not null,

    foreign key (kid) references rkey (id)
    on delete cascade
) strict;

create unique index if not exists
rstring_pk_idx on rstring (kid);

create view if not exists
vstring as
select
    rkey.id as kid, rkey.key, rstring.value,
    datetime(etime/1000, 'unixepoch') as etime,
    datetime(mtime/1000, 'unixepoch') as mtime
from rstring join rkey on rstring.kid = rkey.id and rkey.type = 1
where rkey.etime is null or rkey.etime > unixepoch('subsec');

-- ┌───────────────┐
-- │ Lists         │
-- └───────────────┘
create table if not exists
rlist (
    kid    integer not null,
    pos    real not null,
    elem   blob not null,

    foreign key (kid) references rkey (id)
    on delete cascade
) strict;

create unique index if not exists
rlist_pk_idx on rlist (kid, pos);

create trigger if not exists
rlist_on_update
before update on rlist
for each row
begin
    update rkey set
        version = version + 1,
        mtime = unixepoch('subsec') * 1000
    where id = old.kid;
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
    where id = old.kid;
end;

create view if not exists
vlist as
select
    rkey.id as kid, rkey.key,
    row_number() over w as idx, rlist.elem,
    datetime(etime/1000, 'unixepoch') as etime,
    datetime(mtime/1000, 'unixepoch') as mtime
from rlist join rkey on rlist.kid = rkey.id and rkey.type = 2
where rkey.etime is null or rkey.etime > unixepoch('subsec')
window w as (partition by kid order by pos);

-- ┌───────────────┐
-- │ Hashes        │
-- └───────────────┘
create table if not exists
rhash (
    kid   integer not null,
    field text not null,
    value blob not null,

    foreign key (kid) references rkey (id)
    on delete cascade
) strict;

create unique index if not exists
rhash_pk_idx on rhash (kid, field);

create trigger if not exists
rhash_on_insert
before insert on rhash
for each row
when (
        select count(*) from rhash
        where kid = new.kid and field = new.field
    ) = 0
begin
    update rkey
    set len = len + 1
    where id = new.kid;
end;

create view if not exists
vhash as
select
    rkey.id as kid, rkey.key, rhash.field, rhash.value,
    datetime(etime/1000, 'unixepoch') as etime,
    datetime(mtime/1000, 'unixepoch') as mtime
from rhash join rkey on rhash.kid = rkey.id and rkey.type = 4
where rkey.etime is null or rkey.etime > unixepoch('subsec');

-- ┌───────────────┐
-- │ Sorted sets   │
-- └───────────────┘
create table if not exists
rzset (
    kid    integer not null,
    elem   blob not null,
    score  real not null,

    foreign key (kid) references rkey (id)
    on delete cascade
) strict;

create unique index if not exists
rzset_pk_idx on rzset (kid, elem);

create index if not exists
rzset_score_idx on rzset (kid, score, elem);

create trigger if not exists
rzset_on_insert
before insert on rzset
for each row
when (
        select count(*) from rzset
        where kid = new.kid and elem = new.elem
    ) = 0
begin
    update rkey
    set len = len + 1
    where id = new.kid;
end;

create view if not exists
vzset as
select
    rkey.id as kid, rkey.key, rzset.elem, rzset.score,
    datetime(etime/1000, 'unixepoch') as etime,
    datetime(mtime/1000, 'unixepoch') as mtime
from rzset join rkey on rzset.kid = rkey.id and rkey.type = 5
where rkey.etime is null or rkey.etime > unixepoch('subsec');
