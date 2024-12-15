-- drop table if exists account cascade;

create table if not exists account
(
    id             int            not null,
    type           varchar(12)   not null CHECK(type IN ('credit','checking')),
    version        int            default on null 0,
    balance        numeric(19, 2) not null,
    name           varchar(128)   null,
    allow_negative int            default on null 0,

    primary key (id, type)
);