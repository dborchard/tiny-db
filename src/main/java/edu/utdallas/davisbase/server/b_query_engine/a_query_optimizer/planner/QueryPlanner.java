package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.planner;

import edu.utdallas.davisbase.server.a_frontend.domain.commands.QueryData;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;

public interface QueryPlanner {

    Plan createPlan(QueryData data, Transaction tx);
}
