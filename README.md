# Chaos

A simple JDBC application for comparing the semantics of RC and 1SR isolation 
levels in CockroachDB. Specifically by observing the application impact on:

- Retries (transaction rollbacks with state code 40001)
- Performance (is RC faster than 1SR or not)
- Correctness (P4 lost update and A5B write skew anomalies in particular)

The application is designed to cause workload contention by concurrently 
updating an overlapping set of keys in a conflicting order. This is denied 
under 1SR (manifested as retryable errors) but allowed under RC, thus resulting 
in either the P4 (lost update) or A5B (write skew) anomaly unless a locking 
strategy is applied. That locking strategy is either pessimistic for-update
locks or optimistic locks using CAS with versioning. 

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

## Conflicting Operations

A brief description of the workload which is concurrently updating the same
account table with an overlapping set of keys.

### Lost Update Workload

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

Example using read-committed:

    << Totals >>
    Args: [--rc]
    Using: CockroachDB CCL v23.2.0 (x86_64-pc-linux-gnu, built 2024/01/16 19:30:18, go1.21.5 X:nocoverageredesign)
    Execution time: PT10.170636S
    Total commits: 1 000
    Total fails: 0
    Total retries: 5
    << Timings >>
    Avg time spent in txn: 79,8 ms
    Cumulative time spent in txn: 80193 ms
    Min time in txn: 47,0 ms
    Max time in txn: 354,0 ms
    Tot samples: 1005
    P95 latency 142,0 ms
    P99 latency 189,0 ms
    P99.9 latency 256,0 ms
    << Safety >>
    Using locks (sfu): no
    Using CAS: no
    Isolation level: read committed
    << Verdict >>
    Total initial balance: 250000000.00
    Total final balance: 249999754.75
    250000000.00 != 249999754.75 (ノಠ益ಠ)ノ彡┻━┻
    You just lost 245.25 and may want to reconsider your isolation level!! (or use --sfu or --cas)

Example using serializable:

    << Totals >>
    Args: []
    Using: CockroachDB CCL v23.2.0 (x86_64-pc-linux-gnu, built 2024/01/16 19:30:18, go1.21.5 X:nocoverageredesign)
    Execution time: PT12.892385S
    Total commits: 1 000
    Total fails: 0
    Total retries: 194
    << Timings >>
    Avg time spent in txn: 85,2 ms
    Cumulative time spent in txn: 101761 ms
    Min time in txn: 33,0 ms
    Max time in txn: 313,0 ms
    Tot samples: 1194
    P95 latency 159,0 ms
    P99 latency 219,0 ms
    P99.9 latency 298,0 ms
    << Safety >>
    Using locks (sfu): no
    Using CAS: no
    Isolation level: serializable
    << Verdict >>
    Total initial balance: 250000000.00
    Total final balance: 250000000.00
    You are good! ¯\_(ツ)_/¯̑̑
    
