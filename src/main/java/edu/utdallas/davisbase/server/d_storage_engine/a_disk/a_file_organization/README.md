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