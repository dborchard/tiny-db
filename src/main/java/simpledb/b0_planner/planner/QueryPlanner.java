package simpledb.b0_planner.planner;

import simpledb.b0_planner.plan.Plan;
import simpledb.c_parse.commands.QueryData;
import simpledb.f_tx.Transaction;

public interface QueryPlanner {

    Plan createPlan(QueryData data, Transaction tx);
}
