# Query Optimizer

The Optimizer (Oracle), Query Optimizer (SQL Server, MySQL) or Query Planner (PostgreSQL) translates the SQL statement
to an executable program, in form of an execution plan, very much like a compiler translates source code into an
executable.

## Kinds

There are generally two kinds of Optimizers:

1. Rule Based Optimizer (RBO): Rule Based Optimizers follow a strict set of rules to create an execution plan — e.g.,
   always use an index if possible.

2. Cost Based Optimizer (CBO): Cost Based Optimizers generate many different execution plans, apply a cost model to all
   of them and select the one with the best cost value for execution.