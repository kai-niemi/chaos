# Chaos

[![Java CI with Maven](https://github.com/kai-niemi/chaos/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/kai-niemi/chaos/actions/workflows/maven.yml)

A small JDBC application for comparing the semantics of read-committed (RC) 
and serializable (1SR) isolation levels in CockroachDB, PostgreSQL, MySQL 
and Oracle.

Specifically by observing application impact around:

- Number of retries and time spent retrying: 
  - Frequency of concurrency errors and transient SQL exceptions (code 40001 or 40P01)
- Performance impact by:
  - Weaker vs stronger isolation
  - Pessimistic locking using `FOR UPDATE` or `FOR SHARE`
  - Optimistic locking using compare-and-set (CAS)
- Correctness when exposed to the following anomalies:
  - P4 lost update 
  - P2 non-repeatable / fuzzy read 
  - P3 phantom read 
  - A5A read skew 
  - A5B write skew 

Isolation levels characterised by the phenomena they allow or prevent:

|                  | Default Isolation | P0 D.Write   | P1 D.Read    | P4C C.Lost Update | P4 Lost Update     | P2 Fuzzy Read      | P3 Phantom Read    | A5A Read Skew | A5B Write Skew     |
|------------------|-------------------|--------------|--------------|-------------------|--------------------|--------------------|--------------------|---------------|--------------------|
| Read Uncommitted |                   | Not Possible | Possible     | Possible          | Possible           | Possible           | Possible           | Possible      | Possible           |
| Read Committed   | Oracle,PostgreSQL | Not Possible | Not Possible | Possible          | Possible           | Possible           | Possible           | Possible      | Possible           |
| Cursor Stability |                   | Not Possible | Not Possible | Not Possible      | Sometimes Possible | Sometimes Possible | Possible           | Possible      | Sometimes Possible |
| Repeatable Read  | MySQL             | Not Possible | Not Possible | Not Possible      | Not Possible       | Not Possible       | Possible           | Not Possible  | Not Possible       |
| Snapshot *)      |                   | Not Possible | Not Possible | Not Possible      | Not Possible       | Not Possible       | Sometimes Possible | Not Possible  | Possible           |
| Serializable     | CockroachDB       | Not Possible | Not Possible | Not Possible      | Not Possible       | Not Possible       | Not Possible       | Not Possible  | Not Possible       |

*) Oracle calls this Serializable and PostgreSQL implements this in Repeatable Read.  

# How it works

The application is intentionally designed to cause pathological workload contention 
by concurrently updating an overlapping set of keys in a conflicting order and using
aggregate functions in queries (thus more subject to skewed observations). 

The purpose is to observe the application impact on correctness and performance while
running the workloads and playing around with isolation levels and locking strategy,
as outlined above.

The workloads are simple and centered around a single `account` table simulating 
bank accounts (see more below). In the end of each run, certain consistency checks
are performed to verify that the accounts remain in a correct state and not in 
breaching of any invariants. 

Inconsistent outcomes will occur when runnning in weaker isolation levels than 1SR 
and not using any locking strategy. That is the general tradeoff with weaker isolation, 
that you risk incorrect outcomes unless taking certain measures application-side 
like using optimistic or pessmistic locking. 

## Further Resources

- https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf
- https://blog.acolyer.org/2016/02/24/a-critique-of-ansi-sql-isolation-levels/
- http://www.bailis.org/papers/acidrain-sigmod2017.pdf
- http://www.bailis.org/blog/when-is-acid-acid-rarely/
- https://jepsen.io/consistency

# Building and Running

## Prerequisites

### Building

- Java 17+ JDK
    - https://openjdk.org/projects/jdk/17/
    - https://www.oracle.com/java/technologies/downloads/#java17
- Maven 3+ (optional, embedded wrapper available)
    - https://maven.apache.org/

### Running

- Java 17+ JRE
- CockroachDB 23.2 or later (24.1+ with enterprise license for read-committed)
    - https://www.cockroachlabs.com/docs/releases/
- PostgreSQL 9+ (optional)
- Oracle 23ai Free (optional)
- MySQL 9.x (optional)

## Install the JDK

Ubuntu:

    sudo apt-get install openjdk-17-jdk

MacOS (using sdkman):

    curl -s "https://get.sdkman.io" | bash
    sdk list java
    sdk install java 17.0 (pick version)  

## Database Setup

### CockroachDB Setup

See https://www.cockroachlabs.com/docs/v24.2/start-a-local-cluster for setup instructions.

Ensure that you are using CockroachDB 23.2 or later and enable RC with:

    cockroach sql --insecure -e "SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = 'true'"

Then create the database:

    cockroach sql --insecure -e "create database chaos"

### PostgreSQL Setup

Install PSQL on MacOS using brew:

    brew install postgresql

Starting:

    brew services start postgresql@14

Create the database:

    psql postgres
    $ create database chaos;
    $ quit

Stopping:

    brew services stop postgresql@14

### MySQL Setup

Install MySQL on MacOS using brew:

    brew install mysql

Starting:

    brew services start colima
    brew services start mysql

Create the database:

    mysql -u root
    $ create database chaos;
    $ quit

Stopping:

    brew services stop mysql

### Oracle Setup

Install Oracle 23 Free on MacOS using brew:

    brew install colima
    brew install docker
    colima start --arch x86_64 --memory 4

Starting:

    docker run -d -p 1521:1521 -e ORACLE_PASSWORD=root -v oracle-volume:/opt/oracle/oradata gvenzl/oracle-free

Stopping:

    docker ps -al
    docker stop $(docker ps -a -q)

## Building

Maven is used to build the project, bootstrapped by Tanuki Maven wrapper.

    ./mvnw clean install

## Runing

    java -jar target/chaos.jar --help

For more examples, see workload samples below.

# Workloads

A description of the different anomalies manifested (and prevented) in Chaos.

## Lost Update (P4)

> A lost update happens when one transaction overwrites the changes made by another transaction.

Examples:
    
    java -jar target/chaos.jar --isolation rc lost_update
    java -jar target/chaos.jar --url "jdbc:postgresql://192.168.1.2:5432/chaos" --profile psql lost_update 

This workload concurrently executes the following statements (at minimum 4) using 
explicit transactions and N threads:

    BEGIN; 
    SELECT balance from account where id=1;
    SELECT balance from account where id=2;
    UPDATE account set balance=? where id = 1; -- balace @1 + 5
    UPDATE account set balance=? where id = 2; -- balance @2 - 5
    COMMIT;

The interleaving hopefully results in a conflicting order of operations 
causing the database to rollback transactions with a transient, retryable errors 
(40001 code) when running under 1SR. When running under RC it results in lost updates
(as expected) unless a locking strategy is applied (see --locking).

This workload is unsafe in RC unless using optimistic "locks" through a CAS operation
(version increments) or by using pessimistic `SELECT .. FOR UPDATE/SHARE` locks at read time.

Ex:

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

## Non-Repeatable Read (P2)

Also called a fuzzy read and described as:

> A non-repeatable read occurs, when during the course of a transaction, a row is retrieved twice and the values within the row differ between reads.

Example:

    begin; -- t1
    set transaction isolation level read committed; -- t1
    begin; -- t2
    set transaction isolation level read committed; -- t2
    SELECT balance FROM account WHERE id = 1 AND type = 'checking'; -- t1 100
    SELECT balance FROM account WHERE id = 1 AND type = 'checking'; -- t1 100
    SELECT balance FROM account WHERE id = 1 AND type = 'checking'; -- t1 100
    UPDATE account SET balance = 50 WHERE id = 1 and type='checking'; -- t2
    SELECT balance FROM account WHERE id = 1 AND type = 'checking'; -- t1 100
    commit; -- t2
    SELECT balance FROM account WHERE id = 1 AND type = 'checking'; -- t1 50  (P2 anomaly !!)
    commit; -- t1

This will result in anomalies:

    java -jar target/chaos.jar --profile crdb --isolation rc --selection 10 --accounts 100 --iterations 100 p2

This will result in a correct outcome:

    java -jar target/chaos.jar --profile crdb --isolation 1sr --selection 10 --accounts 100 --iterations 100 p2

## Phantom Read (P3)

> A phantom read occurs when, in the course of a transaction, two identical queries are executed, and the collection of rows returned by the second query is different from the first.

In other words, the predicate is unstable when repeating the read.

Example:

    insert into account (id, type, balance)
    values (1, 'a', 100.00),
           (1, 'b', 100.00),
           (1, 'c', 100.00);
    begin; -- t1
    set transaction isolation level read committed; -- t1
    begin; -- t2
    set transaction isolation level read committed; -- t2
    select * from account where id = 1; -- t1 (3 rows)
    select * from account where id = 1; -- t1 (3 rows)
    insert into account (id, type, balance) values (1, 'd', 100.00); -- t2
    commit; -- t2
    select * from account where id = 1; -- t1 (must be 3 rows and if 4 then its a p3 violation !!)
    commit; -- t1

This will result in anomalies:

    java -jar target/chaos.jar --profile crdb --isolation rc --selection 10 --accounts 100 --iterations 100 p3

This will result in a correct outcome:

    java -jar target/chaos.jar --profile crdb --isolation 1sr --selection 10 --accounts 100 --iterations 100 p3

## Write Skew (A5b)

> Write skew is a phenomenon where two writes are allowed to the same column(s) in 
> a table by two different writers (who have previously read the columns they are updating), 
> resulting in the column having data that is a mix of the two transactions.

Examples:

    java -jar target/chaos.jar --isolation rc --selection 20 write_skew
    java -jar target/chaos.jar --url "jdbc:postgresql://localhost:5432/chaos" --profile psql --isolation rc write_skew 

In this workload, accounts are organized in tuples where the same surrogate id is shared by a
checking and credit account. The composite primary key is `id,type`. The business rule (invariant)
is that the account balances can be negative or positive as long as the sum of both accounts is be >= 0.

| id | type     | balance |
|----|----------|---------|
| 1  | checking | -5.00   |
| 1  | credit   | 10.00   |
| Σ  | -        | +5.00   | 

Write skew can happen if `T1` and `T2` reads the total balance (which is >0 ) and
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

Both transactions are correct in isolation but when put together the total sum is `-5.00` (rule violation)
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

## Read Skew (A5a)

> Read skew is that with two different queries, a transaction reads inconsistent 
> data because between the 1st and 2nd queries, other transactions insert, 
> update or delete data and commit.

Examples:

    java -jar target/chaos.jar --isolation rc --selection 20 read_skew
    java -jar target/chaos.jar --url "jdbc:postgresql://localhost:5432/chaos" --profile psql --isolation rc read_skew 

This workload is similar to write skew where the account balances are read in separate statements
and the sum is expected to remain constant. Under RC without locks, it's allowed to read values
committed by other concurrent transactions and thereby observe deviations. Under 1SR, this is
prevented and results in a transient rollback error.

# Terms of Use

This tool is not supported by Cockroach Labs. Use of this tool is entirely at your
own risk and Cockroach Labs makes no guarantees or warranties about its operation.

See [MIT](LICENSE.txt) for terms and conditions.
