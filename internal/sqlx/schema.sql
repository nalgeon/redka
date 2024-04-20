pragma user_version = 1;

-- keys
create table if not exists
rkey (
    id       integer primary key,
    key      text not null,
    type     integer not null,
	version  integer not null,
    etime    integer,
	mtime    integer not null
);

create unique index if not exists
rkey_key_idx on rkey (key);

create index if not exists
rkey_etime_idx on rkey (etime)
where etime is not null;

create trigger if not exists
rkey_on_type_update
before update of type on rkey
for each row
when old.type is not new.type
begin
    select raise(abort, 'key type mismatch');
end;

-- strings
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

-- hashes
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

create index if not exists
rhash_key_id_idx on rhash (key_id);

create view if not exists
vhash as
  select
    rkey.id as key_id, rkey.key, rhash.field, rhash.value,
	datetime(etime/1000, 'unixepoch') as etime,
	datetime(mtime/1000, 'unixepoch') as mtime
  from rkey join rhash on rkey.id = rhash.key_id
  where rkey.type = 4
    and (rkey.etime is null or rkey.etime > unixepoch('subsec'));

-- set

create table if not exists
    rset (
             key_id integer not null,
             elem blob not null,

             foreign key (key_id) references rkey (id)
    on delete cascade
    );

create unique index if not exists
    rset_pk_idx on rset (key_id, elem);

create view if not exists
        vset as
select
    rkey.id as key_id, rkey.key, rset.elem
               datetime(etime/1000, 'unixepoch') as etime,
        datetime(mtime/1000, 'unixepoch') as mtime
from rkey join rset on rkey.id = rset.key_id
where rkey.type = 3
  and (rkey.etime is null or rkey.etime > unixepoch('subsec'))