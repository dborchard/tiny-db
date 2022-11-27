package edu.utdallas.davisbase.db.query_engine.a_planner.planner.impl;

import edu.utdallas.davisbase.db.frontend.domain.commands.QueryData;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.ProjectPlan;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.SelectPlan;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.SelectWithIndexPlan;
import edu.utdallas.davisbase.db.query_engine.b_metadata.MetadataMgr;
import edu.utdallas.davisbase.db.query_engine.b_metadata.index.IndexInfo;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.Transaction;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.TablePlan;
import edu.utdallas.davisbase.db.query_engine.a_planner.planner.QueryPlanner;
import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;

import java.util.Map;

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

        Plan p = new TablePlan(tx, data.table(), mdm);

        boolean indexFound = false;
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(data.table(), tx);
        for (String fldname : indexes.keySet()) {
            D_Constant val = data.pred().equatesWithConstant(fldname);
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
