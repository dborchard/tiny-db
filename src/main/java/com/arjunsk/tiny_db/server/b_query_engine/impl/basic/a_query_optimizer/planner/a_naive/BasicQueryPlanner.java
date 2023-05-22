package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.a_naive;

import com.arjunsk.tiny_db.server.a_frontend.common.domain.commands.QueryData;
import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.MetadataMgr;
import com.arjunsk.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.C_ProjectPlan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.B_SelectPlan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.A_TablePlan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;

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
