# Chaos

A simple JDBC application for comparing the semantics of RC and 1SR isolation 
levels in CockroachDB. Specifically by observing the application impact on:

- Retries (transaction rollbacks with state code 40001)
- Performance (if RC is faster than 1SR or not)
- Correctness (P4 lost update and A5B write skew anomalies in particular)

The application is designed to cause workload contention by concurrently 
updating an overlapping set of keys in a conflicting order. This is denied 
under 1SR (manifested as retryable errors) but allowed under RC, thus resulting 
in either the P4 (lost update) or A5B (write skew) anomaly unless a locking 
strategy is applied. That locking strategy is either pessimistic for-update
locks or optimistic locks using CAS with versioning. 

It supports both PSQL and CockroachDB v23.2+.

## Schema 

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

## Conflicting Workloads

A brief description of the workload which is concurrently updating the same
account table with an overlapping set of keys.

### Lost Update Workload 

Examples:
    
    ./run.sh lost_update --rc
    ./run.sh --url "jdbc:postgresql://localhost:5432/chaos" --dialect psql --rc lost_update 

This workload concurrently executes the following statements (at minimum 4) using 
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

This workload is unsafe in RC unless using optimistic "locks" through a CAS operation 
(version increments) or by using pessimistic `SELECT .. FOR UPDATE` locks at read time.

### Write Skew Workload

Examples:

    ./run.sh --rc --selection 20 write_skew
    ./run.sh --url "jdbc:postgresql://localhost:5432/chaos" --dialect psql --rc write_skew 

Accounts are organized in tuples where the same id is shared by a checking and credit 
account. The composite primary key is `id,type`. The rule is that the account balances can be
negative or positive as long as the sum of both accounts is be >= 0.

| id | type     | balance |
|----|----------|---------|
| 1  | checking | -5.00   |
| 1  | credit   | 10.00   |
| Σ  | -        | +5.00   | 

The write skew can happen if `T1` and `T2` reads the total balance (which is >0 ) and 
independently writes a new balance to different rows.

Preset:

    insert into account (id, type, balance) values(1, 'checking', -5.00);
    insert into account (id, type, balance) values(1, 'credit', 10.00);

Assume transaction `T1` and `T2`:

| T1                                                                                 | T2                                                                                  |
|------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| begin;                                                                             | begin;                                                                              |
| set transaction isolation level read committed;                                    | set transaction isolation level read committed;                                     |
|                                                                                    | select sum(balance) from account where id=1; -- 5.00                                |
| select sum(balance) from account where id=1; -- 5.00                               |                                                                                     |
| update account set balance=balance-5 where id=1 and type='credit';  -- ok sum is 0 |                                                                                     | 
|                                                                                    | update account set balance=balance-5 where id=1 and type='checking'; -- ok sum is 0 | 
| commit; --ok                                                                       |                                                                                     | 
|                                                                                    | commit; -- ok (not allowed in 1SR)                                                  | 

Both transactions were correct in isolation but when put together the total sum is `-5.00` (rule violation) 
since they were both allowed to commit. This subtle anomaly is called write skew, which is prevented 
in 1SR but allowed in RC.

In summary, this workload concurrently executes the following statements (using pseudo-code):

    BEGIN; 
    SELECT sum(balance) total from account where id=<random from selection>; -- expect 2 rows
    if (total - random_amount > 0) -- app rule check
        (either)
            UPDATE account set balance=balance-? where id = 1 and type='checking'; 
        (or)
            UPDATE account set balance=balance-? where id = 2 and type='credit'; 
    endif
    COMMIT;

This workload is therefore unsafe in RC unless using optimistic "locking" through a CAS operation
(version increments). Notice that pessimistic locks can't be used due to the 
aggregate `sum` function.

**Hint:** To have a greater chance to observe anomalies in RC, decrease the `--selection` and/or
increase `--iterations` with 10x.

### Read Skew Workload

Examples:

    ./run.sh --rc --selection 20 read_skew
    ./run.sh --url "jdbc:postgresql://localhost:5432/chaos" --dialect psql --rc read_skew 

This workload is similar to write skew where the account balances
are read in separate statements where the sum is expected to remain
constant. Under RC without locks its allowed to read values 
committed by other concurrent transactions and therefore observe
deviations. 

## CockroachDB Setup

Ensure that you are using CockroachDB 23.2 or later and then enable RC with:

    SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = 'true';

## Build and Run

Install the JDK:

Ubuntu
    
    sudo apt-get install openjdk-17-jdk

MacOS using sdkman

    curl -s "https://get.sdkman.io" | bash
    sdk list java
    sdk install java 17.0 (pick version)  

Build:

    ./mvnw clean install

Run:

    java -jar target/chaos.jar --help
