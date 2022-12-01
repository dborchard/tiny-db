package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.a_naive;

import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.QueryData;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.C_ProjectPlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.B_SelectPlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.A_TablePlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.davisbase.server.d_storage_engine.common.transaction.Transaction;

/**
 * The Query Planner without Indexes and cost considerations.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class BasicQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public BasicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    public Plan createPlan(QueryData data, Transaction tx) {
        //Step 1: Create the plan
        Plan p = new A_TablePlan(tx, data.table(), mdm);

        //Step 3: Add a selection plan for the predicate
        p = new B_SelectPlan(p, data.pred());

        //Step 4: Project on the field names
        p = new C_ProjectPlan(p, data.fields());
        return p;
    }
}
