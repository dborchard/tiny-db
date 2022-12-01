package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner;

import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.QueryData;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.d_storage_engine.common.transaction.Transaction;


/**
 * The Query Planner
 *
 * @author Edward Sciore
 */
public interface QueryPlanner {

    Plan createPlan(QueryData data, Transaction tx);
}
