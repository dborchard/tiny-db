package edu.utdallas.davisbase.server.query_engine.a_planner.planner;

import edu.utdallas.davisbase.server.frontend.domain.commands.QueryData;
import edu.utdallas.davisbase.server.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.server.storage_engine.Transaction;

public interface QueryPlanner {

    Plan createPlan(QueryData data, Transaction tx);
}
