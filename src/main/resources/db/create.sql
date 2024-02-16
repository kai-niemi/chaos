-- drop table if exists account;

create table if not exists account
(
    id      int            not null primary key default unordered_unique_rowid(),
    version int            not null             default 0,
    balance numeric(19, 2) not null,
    name    varchar(128)   not null
);

-- alter table account
--     add constraint check_account_positive_balance check (balance >= 0);

