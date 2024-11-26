--
-- Demo of CockroachDB serializable and read-committed isolation with lock promotion.
-- Subject is the P4 lost update anomaly.
--

-- Setup
-- drop table account;
create table account
(
    id             int            not null default unordered_unique_rowid(),
    balance        numeric(19, 2) not null,

    primary key (id)
);

-- Run between each test
delete from account where 1=1;
insert into account (id, balance)
values (1,  100.00),
       (2,  200.00);

--
-- Test cases
--

-- CockroachDB "serializable" prevents Lost Update (P4):
begin; set transaction isolation level serializable; -- T1
begin; set transaction isolation level serializable; -- T2
show transaction_isolation; -- t1
show transaction_isolation; -- t2
select * from account where id = 1; -- T1
select * from account where id = 1; -- T2
update account set balance = 11 where id = 1; -- T1
update account set balance = 22 where id = 1; -- T2, BLOCKS
commit; -- T1. T2 now prints out "ERROR: restart transaction: TransactionRetryWithProtoRefreshError: WriteTooOldError"
abort;  -- T2. There's nothing else we can do, this transaction has failed

-- CockroachDB "read committed" does not prevent Lost Update (P4):
begin; set transaction isolation level read committed; -- T1
begin; set transaction isolation level read committed; -- T2
show transaction_isolation; -- t1
show transaction_isolation; -- t2
select * from account where id = 1; -- T1
select * from account where id = 1; -- T2
update account set balance = 11 where id = 1; -- T1
update account set balance = 22 where id = 1; -- T2, BLOCKS
commit; -- T1. This unblocks T2, which read 100, so T1's update to 11 is overwritten (lost update)
commit; -- T2

-- CockroachDB "read committed" with SFU does prevent Lost Update (P4):
begin; set transaction isolation level read committed; -- T1
begin; set transaction isolation level read committed; -- T2
show transaction_isolation; -- t1
show transaction_isolation; -- t2
select * from account where id = 1 FOR UPDATE; -- T1
select * from account where id = 1 FOR UPDATE; -- T2, BLOCKS
update account set balance = 11 where id = 1; -- T1
commit; -- T1. This unblocks T2, which reads T1's update (11)
update account set balance = 22 where id = 1; -- T2
commit; -- T2
