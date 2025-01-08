-- drop table if exists account cascade;

create table if not exists account
(
    id             int            not null,
    type           varchar(32)    not null,
    version        int default on null 0,
    balance        numeric(19, 2) not null,
    name           varchar(128)   null,
    allow_negative int default on null 0,

    primary key (id, type)
);