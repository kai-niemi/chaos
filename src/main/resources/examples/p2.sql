delete from account where 1=1;
insert into account (id, type, balance)
values (1, 'checking', 100.00);

begin; -- t1
set transaction isolation level read committed; -- t1
begin; -- t2
set transaction isolation level read committed; -- t2
SELECT balance FROM account WHERE id = 1 AND type = 'checking'; -- t1 100
UPDATE account SET balance = 50 WHERE id = 1 and type='checking'; -- t2
SELECT balance FROM account WHERE id = 1 AND type = 'checking'; -- t1 100
commit; -- t2
SELECT balance FROM account WHERE id = 1 AND type = 'checking'; -- t1 if 50 then P2!!!
commit; -- t1
