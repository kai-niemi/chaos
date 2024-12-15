delete from account where 1=1;
insert into account (id, type, balance)
values (1, 'a', 100.00),
       (2, 'b', 200.00);

begin; set transaction isolation level read committed; -- T1
begin; set transaction isolation level read committed; -- T2
select * from account where id = 1 and type = 'a'; -- T1. Shows 1 => 100
select * from account where id = 1 and type = 'a'; -- T2. Shows 1 => 100
select * from account where id = 2 and type = 'b'; -- T2. Shows 2 => 200
update account set balance = 12 where id = 1 and type = 'a'; -- T2
update account set balance = 18 where id = 2 and type = 'b'; -- T2
commit; -- T2
select * from account where id = 2 and type = 'b'; -- T1. Shows 2 => 18 which is A5b !!
commit; -- T1