## DavisBase

A tiny database that supports Btree Index.

## Features

- Basic Planners
- Index Planners
- CLI interface
- File Manager
- Record Page

## Sample Queries
- Without Index
```shell
create table T1 ( A int, B varchar(9) );
insert into T1 (A, B) values (1, 'Alice');
insert into T1 (A, B) values (2, 'Bob');
select a,b from T1;
select a,b from T1 where a=1;
```

- With Index
```shell
create table T2 ( A int, B varchar(9) );
create index A_IDX on T2(A);
insert into T2 (A, B) values (1, 'Alice');
insert into T2 (A, B) values (2, 'Bob');
select a,b from T2;
select a,b from T2 where a=1;
```

## TODO

- Recovery Manager (WAL)
- Transactions
- Concurrency Manager
- Buffer Manager

## Notes

This work is a derived from [SimpleDB](http://cs.bc.edu/~sciore/simpledb/)