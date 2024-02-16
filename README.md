# Chaos

A small JDBC app for comparing the semantics of RC and 1SR 
isolation level in CockroachDB. Specifically by observing 
application impact in regard to:

- Retries
- Performance
- Correctness (P4 lost update specifically)

The workload contention can be adjusted as well as locking
strategies to ensure a correct outcome using both isolation
levels.

The workload is subject to P4 lost update anomaly under RC.

Ex:

    BEGIN; --T1
    SELECT balance from account where id=1;
    SELECT balance from account where id=2;
    SELECT balance from account where id=3;
    SELECT balance from account where id=4;

    UPDATE account set balance=? where id = 1; -- (balace + 5)
    UPDATE account set balance=? (-5) where id = 2;
    UPDATE account set balance=? (+5) where id = 3;
    UPDATE account set balance=? (-5) where id = 4;
    COMMI;