package simpledb.b0_planner.planner.impl;

import simpledb.b0_planner.plan.Plan;
import simpledb.b0_planner.plan.impl.ProjectPlan;
import simpledb.b0_planner.plan.impl.SelectPlan;
import simpledb.b0_planner.plan.impl.TablePlan;
import simpledb.b0_planner.planner.QueryPlanner;
import simpledb.c_parse.commands.QueryData;
import simpledb.b1_metadata.MetadataMgr;
import simpledb.f_tx.Transaction;


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
