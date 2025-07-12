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
    id      serial primary key,
    key     text not null,
    type    integer not null,
    version integer not null,
    etime   bigint,
    mtime   bigint not null,
    len     integer
);

create unique index if not exists
rkey_key_idx on rkey (key);

create index if not exists
rkey_etime_idx on rkey (etime)
where etime is not null;

create or replace view
vkey as
select
    id as kid, key, type, len,
    to_timestamp(etime/1000) as etime,
    to_timestamp(mtime/1000) as mtime
from rkey
where rkey.etime is null or rkey.etime > (extract(epoch from now()) * 1000);

-- ┌───────────────┐
-- │ Strings       │
-- └───────────────┘
create table if not exists
rstring (
    kid    integer not null references rkey(id) on delete cascade,
    value  bytea not null,
    primary key (kid)
);

create or replace view
vstring as
select
    rkey.id as kid, rkey.key, rstring.value,
    to_timestamp(etime/1000) as etime,
    to_timestamp(mtime/1000) as mtime
from rstring
join rkey on rstring.kid = rkey.id and rkey.type = 1
where rkey.etime is null or rkey.etime > (extract(epoch from now()) * 1000);

-- ┌───────────────┐
-- │ Lists         │
-- └───────────────┘
create table if not exists
rlist (
    rowid serial primary key,
    kid    integer not null references rkey(id) on delete cascade,
    pos    double precision not null,
    elem   bytea not null
);

create unique index if not exists
rlist_uniq_idx on rlist (kid, pos);

create or replace function
rlist_on_update_func()
returns trigger as $$
begin
    update rkey set
        version = version + 1,
        mtime = (extract(epoch from now()) * 1000)::bigint
    where id = old.kid;
    return new;
end;
$$ language plpgsql;

drop trigger if exists rlist_on_update on rlist;
create trigger rlist_on_update
before update on rlist
for each row
execute function rlist_on_update_func();

create or replace function
rlist_on_delete_func()
returns trigger as $$
begin
    update rkey set
        version = version + 1,
        mtime = (extract(epoch from now()) * 1000)::bigint,
        len = len - 1
    where id = old.kid;
    return old;
end;
$$ language plpgsql;

drop trigger if exists rlist_on_delete on rlist;
create trigger rlist_on_delete
before delete on rlist
for each row
execute function rlist_on_delete_func();

create or replace view
vlist as
select
    rkey.id as kid, rkey.key,
    row_number() over w as idx, rlist.elem,
    to_timestamp(etime/1000) as etime,
    to_timestamp(mtime/1000) as mtime
from rlist
join rkey on rlist.kid = rkey.id and rkey.type = 2
where rkey.etime is null or rkey.etime > (extract(epoch from now()) * 1000)
window w as (partition by rlist.kid order by rlist.pos);

-- ┌───────────────┐
-- │ Sets          │
-- └───────────────┘
create table if not exists
rset (
    rowid serial primary key,
    kid    integer not null references rkey(id) on delete cascade,
    elem   bytea not null
);

create unique index if not exists
rset_uniq_idx on rset (kid, elem);

create or replace function
rset_on_insert_func()
returns trigger as $$
begin
    update rkey
    set len = len + 1
    where id = new.kid;
    return new;
end;
$$ language plpgsql;

drop trigger if exists rset_on_insert on rset;
create trigger rset_on_insert
after insert on rset
for each row
execute function rset_on_insert_func();

create or replace view
vset as
select
    rkey.id as kid, rkey.key, rset.elem,
    to_timestamp(etime/1000) as etime,
    to_timestamp(mtime/1000) as mtime
from rset
join rkey on rset.kid = rkey.id and rkey.type = 3
where rkey.etime is null or rkey.etime > (extract(epoch from now()) * 1000);

-- ┌───────────────┐
-- │ Hashes        │
-- └───────────────┘
create table if not exists
rhash (
    rowid serial primary key,
    kid   integer not null references rkey(id) on delete cascade,
    field text not null,
    value bytea not null
);

create unique index if not exists
rhash_uniq_idx on rhash (kid, field);

create or replace function
rhash_on_insert_func()
returns trigger as $$
begin
    if (
        select count(*) from rhash
        where kid = new.kid and field = new.field
    ) = 0 then
        update rkey
        set len = len + 1
        where id = new.kid;
    end if;
    return new;
end;
$$ language plpgsql;

drop trigger if exists rhash_on_insert on rhash;
create trigger rhash_on_insert
before insert on rhash
for each row
execute function rhash_on_insert_func();

create or replace view
vhash as
select
    rkey.id as kid, rkey.key, rhash.field, rhash.value,
    to_timestamp(etime/1000) as etime,
    to_timestamp(mtime/1000) as mtime
from rhash
join rkey on rhash.kid = rkey.id and rkey.type = 4
where rkey.etime is null or rkey.etime > (extract(epoch from now()) * 1000);

-- ┌───────────────┐
-- │ Sorted sets   │
-- └───────────────┘
create table if not exists rzset (
    rowid serial primary key,
    kid    integer not null references rkey(id) on delete cascade,
    elem   bytea not null,
    score  double precision not null
);

create unique index if not exists
rzset_uniq_idx on rzset (kid, elem);

create index if not exists
rzset_score_idx on rzset (kid, score, elem);

create or replace function
rzset_on_insert_func()
returns trigger as $$
begin
    if (
        select count(*) from rzset
        where kid = new.kid and elem = new.elem
    ) = 0 then
        update rkey
        set len = len + 1
        where id = new.kid;
    end if;
    return new;
end;
$$ language plpgsql;

drop trigger if exists rzset_on_insert on rzset;
create trigger rzset_on_insert
before insert on rzset
for each row
execute function rzset_on_insert_func();

create or replace view vzset as
select
    rkey.id as kid, rkey.key, rzset.elem, rzset.score,
    to_timestamp(etime/1000) as etime,
    to_timestamp(mtime/1000) as mtime
from rzset
join rkey on rzset.kid = rkey.id and rkey.type = 5
where rkey.etime is null or rkey.etime > (extract(epoch from now()) * 1000);
