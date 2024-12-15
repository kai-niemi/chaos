delete from account where 1=1;
insert into account (id, type, balance)
values (1, 'a', 100.00),
       (1, 'b', 100.00),
       (1, 'c', 100.00);

begin; set transaction isolation level read committed ; -- T1
begin; set transaction isolation level read committed ; -- T2
select * from account where id in (1,2) and type in ('a','b'); -- T1
select * from account where id in (1,2) and type in ('a','b'); -- T2
update account set balance = 11 where id = 1; -- T1
update account set balance = 21 where id = 2; -- T2
commit; -- T1
commit; -- T2 (A5b if accepted !!)