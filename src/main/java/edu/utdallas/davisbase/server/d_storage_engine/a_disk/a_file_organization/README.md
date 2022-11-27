# Page Storage Architecture

Different DBMS manage pages in files on disk in different ways.

## Types

- Heap File Organization (Our Approach)
- Tree File Organization (Davis Base Original Requirement)
- Sorted File Organization (SST)
- Hashing File Organization

## NOTE

In our case Record is nothing but a

- Key Value pair, ie <RecordID, Record>
- Here Record is {Column1: Value1, Column2: Value2, ...}
- Note that, we are using traditional rowFamily
- Had it been ColumnFamily, the approach would have been different

## Columnar Family vs Wide-Column(Apache Cassandra) vs Apache Parquet

https://stackoverflow.com/questions/63179561/wide-column-vs-column-family-vs-columnar-vs-column-oriented-db-definition
https://stackoverflow.com/questions/62010368/what-exactly-is-a-wide-column-store

- Columnar Family: Each Column is a separate LSM tree
- Wide Column: Stores column value with multiple parameters stitched together
- Apache Parquet: Creates a row group and then stores columns together.