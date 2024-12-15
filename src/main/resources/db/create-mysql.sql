-- drop table if exists account;

create table if not exists account
(
    id             int                        not null AUTO_INCREMENT,
    type           enum('credit', 'checking') not null,
    version        int                        not null default 0,
    balance        numeric(19, 2)             not null,
    name           varchar(128)               null,
    allow_negative integer                    not null default 0,

    primary key (id, type)
);
