package simpledb.b0_planner.planner.impl;

import simpledb.b0_planner.plan.Plan;
import simpledb.b0_planner.plan.impl.ProjectPlan;
import simpledb.b0_planner.planner.QueryPlanner;
import simpledb.b1_metadata.MetadataMgr;
import simpledb.c_parse.commands.QueryData;
import simpledb.f_tx.Transaction;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 *
 * @author Edward Sciore
 */
public class BetterQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public BetterQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }


    public Plan createPlan(QueryData data, Transaction tx) {

        TablePlanner tp = new TablePlanner(data.table(), data.pred(), tx, mdm);
        Plan currentPlan = tp.makeSelectPlan();
        return new ProjectPlan(currentPlan, data.fields());
    }
    //test

}
