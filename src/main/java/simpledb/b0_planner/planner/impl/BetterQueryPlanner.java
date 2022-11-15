package simpledb.b0_planner.planner.impl;

import simpledb.b0_planner.plan.Plan;
import simpledb.b0_planner.plan.impl.ProjectPlan;
import simpledb.b0_planner.plan.impl.SelectPlan;
import simpledb.b0_planner.plan.impl.SelectWithIndexPlan;
import simpledb.b0_planner.plan.impl.TablePlan;
import simpledb.b0_planner.planner.QueryPlanner;
import simpledb.b1_metadata.MetadataMgr;
import simpledb.b1_metadata.index.IndexInfo;
import simpledb.c_parse.commands.QueryData;
import simpledb.d_scans.domains.Constant;
import simpledb.f_tx.Transaction;

import java.util.Map;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 *
 * @author Edward Sciore
 */
public class BetterQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;
    private Map<String, IndexInfo> indexes;

    public BetterQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }


    public Plan createPlan(QueryData data, Transaction tx) {

        Plan p = new TablePlan(tx, data.table(), mdm);

        boolean indexFound = false;
        for (String fldname : mdm.getIndexInfo(data.table(), tx).keySet()) {
            Constant val = data.pred().equatesWithConstant(fldname);
            if (val != null) {
                IndexInfo ii = indexes.get(fldname);
                p = new SelectWithIndexPlan(p, ii, val);

                indexFound = true;
                System.out.println("index on " + fldname + " used");
                break;
            }
        }

        if (!indexFound) p = new SelectPlan(p, data.pred());

        p = new ProjectPlan(p, data.fields());
        return p;
    }
}
