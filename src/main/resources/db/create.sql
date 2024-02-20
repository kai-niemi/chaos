-- drop table if exists account;

create type if not exists account_type as enum ('credit', 'checking');

create table if not exists account
(
    id      int            not null default unordered_unique_rowid(),
    type    account_type   not null,
    version int            not null default 0,
    balance numeric(19, 2) not null,
    name    varchar(128)   not null,

    primary key (id, type)
);

