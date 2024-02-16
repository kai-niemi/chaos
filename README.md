# Chaos

A small JDBC application for comparing the semantics of RC and 1SR isolation 
levels in CockroachDB. Specifically by observing the application impact on:

- Retries
- Performance
- Correctness (P4 lost update in particular)

The application is designed to produce contention by concurrently updating an 
overlapping set of keys in a conflicting order, which is denied under 1SR 
but allowed under RC, thus resulting in P4 (lost update) anomalies unless 
a locking strategy is applied. 

That strategy is either optimistic "locking" 
through a CAS operation (version increments) or pessimistic locking 
using actual `FOR UPDATE` locks.

## Schema 

    create table if not exists account
    (
        id      int            not null primary key default unordered_unique_rowid(),
        version int            not null             default 0,
        balance numeric(19, 2) not null,
        name    varchar(128)   not null
    );

## Conflicting Operations

A brief description of the workload which is concurrently updating the same
account table with an overlapping set of keys.

It concurrently executes the following statements (at minimum 4) using 
explicit transactions and N threads:

    BEGIN; 
    SELECT balance from account where id=1;
    SELECT balance from account where id=2;
    UPDATE account set balance=? where id = 1; -- balace @1 + 5
    UPDATE account set balance=? where id = 2; -- balance @2 - 5
    COMMIT;

This type of interleaving then hopefully results in a conflicting order 
of operations causing the database to rollback transactions with a transient, 
retryable errors (40001 code) when running under 1SR. When running under RC
it results in lost updates (expected).

Something like the following when using just two concurrent transactions (T1 and T2):

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

The level of contention can be adjusted by increasing the number of account tuples,
number of concurrent executors or by reducing the selection of account IDs involved 
in the interleaving. This allows for creating a high number of accounts spanning 
many ranges while still being able to cause contention.

## CockroachDB Setup

Ensure that you are using CockroachDB 23.2 or later and then enable RC with:

    SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = 'true';

## Build and Run

Install the JDK:

Ubuntu:
    
    sudo apt-get install openjdk-17-jdk

MacOS using sdkman:

    curl -s "https://get.sdkman.io" | bash
    sdk list java
    sdk install java 17.0 (pick version)  

Build:

    ./mvnw clean install

Run:

    java -jar target/chaos.jar --help