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

-- strings
create table if not exists
rstring (
    key_id integer,
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
	case rkey.type when 1 then 'string' else 'unknown' end as type,
	datetime(etime/1000, 'unixepoch', 'utc') as etime,
	datetime(mtime/1000, 'unixepoch', 'utc') as mtime
  from rkey join rstring on rkey.id = rstring.key_id
  where rkey.type = 1
    and (rkey.etime is null or rkey.etime > unixepoch('subsec'));
