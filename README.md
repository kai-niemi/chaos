# Chaos

A small JDBC app for comparing the semantics of RC and 1SR isolation 
level in CockroachDB. Specifically by observing application impact 
in regard to:

- Retries
- Performance
- Correctness (P4 lost update specifically)

It produces contention by updating overlapping set of keys 
in a conflicting order which is denied under 1SR but allowed
under RC and then resulting in P4 lost updates unless a locking
strategy is applied. That strategy is either optimistic locks
using a CAS operation or by using `FOR UPDATE` locks.

## Schema 

    create table if not exists account
    (
        id      int            not null primary key default unordered_unique_rowid(),
        version int            not null             default 0,
        balance numeric(19, 2) not null,
        name    varchar(128)   not null
    );

## Conflicting operations

Concurrently with N threads:

    BEGIN; 
    SELECT balance from account where id=1;
    SELECT balance from account where id=2;
    UPDATE account set balance=? where id = 1; -- balace @1 + 5
    UPDATE account set balance=? where id = 2; -- balance @2 - 5
    COMMIT;

Which means the interleaving can result in a conflicting ordering,
something like with only two concurrent transactions (T1 and T2):

    BEGIN; --T1 
    BEGIN; --T2 
    SELECT balance from account where id=1; --T1
    SELECT balance from account where id=1; --T2
    SELECT balance from account where id=2; --T1
    UPDATE account set balance=? where id = 1; -- T1
    SELECT balance from account where id=2; --T2
    UPDATE account set balance=? where id = 2; -- T1
    COMMIT; --T1
    UPDATE account set balance=? where id = 1; -- T2
    UPDATE account set balance=? where id = 2; -- T2
    COMMIT; --T2

The contention level can be adjusted by increasing the number of
account tuples or by reducing the selection of account IDs involved in
the interleaving. This enables creating a high number of accounts
spanning many ranges while still causing contention.