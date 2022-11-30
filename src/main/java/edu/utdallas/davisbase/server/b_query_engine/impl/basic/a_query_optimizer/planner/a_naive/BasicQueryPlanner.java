package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.a_naive;

import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.QueryData;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.ProjectPlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.SelectPlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.TablePlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.c_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;

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
        Plan p = new TablePlan(tx, data.table(), mdm);

        //Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());

        //Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }
}
