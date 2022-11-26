package edu.utdallas.davisbase.query_engine.b0_planner.planner;

import edu.utdallas.davisbase.query_engine.b0_planner.plan.Plan;
import edu.utdallas.davisbase.query_engine.c_parse.domain.commands.QueryData;
import edu.utdallas.davisbase.query_engine.f_tx.Transaction;

public interface QueryPlanner {

    Plan createPlan(QueryData data, Transaction tx);
}
