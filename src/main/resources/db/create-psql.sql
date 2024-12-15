-- drop table if exists account cascade;
-- drop sequence if exists account_seq;

create sequence if not exists account_seq
    increment by 1 cache 64;

create table if not exists account
(
    id             int            not null default nextval('account_seq'),
    type           varchar(32)    not null,
    version        int            not null default 0,
    balance        numeric(19, 2) not null,
    name           varchar(128)   null,
    allow_negative integer        not null default 0,

    primary key (id, type)
);
