## Architecture

We will be developing TinyDB in a modular structure with the below architecture.

**Tiny DB**: This will be the parent project embedding all the subsequent modules.
This will be the final executable deliverable that we will run to see the DavisBase in
action. This module can also support JDBC if time permits.

**Query Front End**: This layer is going to parse SQL, Tokenize the inputs, and sends it to
the next layer, ie Query Engine. The layer is currently going to support the SHOW,
SELECT, CREATE, INSERT, DELETE, UPDATE, and DROP. For this project, we limit our
scope to the MySQL syntax. Later on, we can extend Query Front End to support
Postgres, MySQL, or any other Query Language Layer. We have used ANTRL to do the
SQL Parsing.

**Query Engine**: This layer is responsible for calculating JOIN cost and finding an optimal
query plan. We used rule base- query optimizer, that will use Index if the field has been
indexed. We also had plans of trying out Calcite as a Query Optimizer layer but didn’t get
enough time.

**Storage Engine**: This is a key-value store module that can talk to the underlying file
system and perform CRUD(Create Read Update Delete) operations on database files. We are using Heap
Page Organization, instead of the traditional Tree Page Organization. Since
we create a B+Tree index on top of the Heap based data block, we get similar
performance. B+Tree will have ColumnValue as the key and RecordId (Contains BlockID
& Offset within the Block) as Value.

## Libraries & References

- ANTRL (https://www.antlr.org/)
  ANTRL is a Parser for parsing the SQL and extracting SQL details for the Query
  Planner. We are using the compiled MySQL parser from shardingsphere.
- Apache Calcite (Not implemented Fully.)
  Calcite is a Production ready Query Planner + Query Parser that can be integrated
  with a storage engine to execute the Query.
- B+Tree Library (https://github.com/davidmoten/bplustree)
  Btree library to create on-disk Btree index.
- SimpleDB (http://cs.bc.edu/~sciore/simpledb/)
  The SimpleDB Project set a base for most of the Project structure.
- Lombok (https://projectlombok.org/)
  Used for creating Getters and Setters for Domain Objects.