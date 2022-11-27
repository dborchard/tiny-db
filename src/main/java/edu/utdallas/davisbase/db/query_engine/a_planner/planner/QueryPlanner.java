package edu.utdallas.davisbase.db.query_engine.a_planner.planner;

import edu.utdallas.davisbase.db.frontend.domain.commands.QueryData;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.storage_engine.b_transaction.Transaction;

public interface QueryPlanner {

    Plan createPlan(QueryData data, Transaction tx);
}
