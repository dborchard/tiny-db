package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.b_rule_base;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.QueryData;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.ProjectPlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.SelectPlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.SelectWithIndexPlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.TablePlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.c_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.c_catalog.index.IndexInfo;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;

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
