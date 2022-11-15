package edu.utdallas.davisbase.b0_planner.planner;

import edu.utdallas.davisbase.b0_planner.plan.Plan;
import edu.utdallas.davisbase.c_parse.commands.QueryData;
import edu.utdallas.davisbase.f_tx.Transaction;

public interface QueryPlanner {

    Plan createPlan(QueryData data, Transaction tx);
}
