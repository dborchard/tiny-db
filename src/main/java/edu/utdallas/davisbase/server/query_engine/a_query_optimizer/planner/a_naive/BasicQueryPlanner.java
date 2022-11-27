package edu.utdallas.davisbase.server.query_engine.a_query_optimizer.planner.a_naive;

import edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.impl.ProjectPlan;
import edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.impl.SelectPlan;
import edu.utdallas.davisbase.server.storage_engine.Transaction;
import edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.impl.TablePlan;
import edu.utdallas.davisbase.server.query_engine.a_query_optimizer.planner.QueryPlanner;
import edu.utdallas.davisbase.server.frontend.domain.commands.QueryData;
import edu.utdallas.davisbase.server.query_engine.b_metadata.MetadataMgr;


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
