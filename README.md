## Tiny

A tiny database that supports Btree Index, Planner and Parser.

## Features

- Frontend
  - Naive Parser 
  - ANTLR MySQL Parser (ShardingSphere Parser Library)

- Query Engine
  - Basic Query Engine (Supporting Projection, Selection etc)
  - Rule Based Planners (use BTree Index if available on that field)
  - Calcite backed Query Engine (Currently supports ScannableTable and not ModifiableTable)
  - Calcite Optimizer [Todo]

- Index
  - Naive B+Tree Index
  - Library backed B+Tree Index (davidmoten bplustree library, Delete not supported by library)
  
- Storage Engine
  - File Manager, Block, Page

- CLI interface

## Sample Queries

NOTE: Delete the `tinydb` data directory to start fresh. 

- Without Index
```shell
create table T1 ( A int, B varchar(9) );
insert into T1 (A, B) values (1, 'Alice');
insert into T1 (A, B) values (2, 'Bob');
select A,B from T1;
select A,B from T1 where A=1;
```
Output
```shell
>
+---+-------+
| a | b     |
+---+-------+
| 1 | Alice |
| 2 | Bob   |
+---+-------+
>
+---+-------+
| a | b     |
+---+-------+
| 1 | Alice |
+---+-------+
```

- With Index
```shell
create table T2 ( A int, B varchar(9) );
create index A_IDX on T2(A);
insert into T2 (A, B) values (1, 'Alice');
insert into T2 (A, B) values (2, 'Bob');
select A,B from T2;
select A,B from T2 where A=1;
```

```shell
>
+---+-------+
| a | b     |
+---+-------+
| 1 | Alice |
| 2 | Bob   |
+---+-------+

> index on a used
+---+-------+
| a | b     |
+---+-------+
| 1 | Alice |
+---+-------+
```
## TODO

- Recovery Manager (WAL)
- Transactions
- Concurrency Manager
- Buffer Manager

## Notes

This work is a derived from [SimpleDB](http://cs.bc.edu/~sciore/simpledb/)

## Current Limitations
- Not implemented Primary Key, Unique Key etc.
- If we create index after the data is inserted, there is some anomaly.
- Currently only supports Varchar, int.