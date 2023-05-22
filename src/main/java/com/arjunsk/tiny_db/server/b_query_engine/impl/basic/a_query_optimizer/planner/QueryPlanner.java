package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner;

import com.arjunsk.tiny_db.server.a_frontend.common.domain.commands.QueryData;
import com.arjunsk.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;


/**
 * The Query Planner
 *
 * @author Edward Sciore
 */
public interface QueryPlanner {

    Plan createPlan(QueryData data, Transaction tx);
}
