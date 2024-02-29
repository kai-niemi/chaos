drop table if exists account cascade ;
drop type if exists account_type;
drop sequence if exists account_seq;

create type account_type as enum ('credit', 'checking');

create sequence if not exists account_seq
    increment by 1 cache 64;

create table if not exists account
(
    id             int            not null default nextval('account_seq'),
    type           account_type   not null,
    version        int            not null default 0,
    balance        numeric(19, 2) not null,
    name           varchar(128)   not null,
    allow_negative integer        not null default 0,

    primary key (id, type)
);
