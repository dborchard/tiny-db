package edu.utdallas.davisbase.db.query_engine.a_planner.planner.impl;

import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.ProjectPlan;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.SelectPlan;
import edu.utdallas.davisbase.db.storage_engine.Transaction;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.TablePlan;
import edu.utdallas.davisbase.db.query_engine.a_planner.planner.QueryPlanner;
import edu.utdallas.davisbase.db.frontend.domain.commands.QueryData;
import edu.utdallas.davisbase.db.query_engine.b_metadata.MetadataMgr;


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
