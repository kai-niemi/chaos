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
