package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import com.arjunsk.tiny_db.server.a_frontend.common.domain.clause.A_Predicate;
import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.b_execution_engine.A_Select_RWRecordScan;
import com.arjunsk.tiny_db.server.d_storage_engine.RORecordScan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;

/**
 * The Plan class corresponding to the <i>select</i>
 * relational algebra operator.
 *
 * @author Edward Sciore
 */
public class B_SelectPlan implements Plan {
    private Plan p;
    private A_Predicate pred;

    public B_SelectPlan(Plan p, A_Predicate pred) {
        this.p = p;
        this.pred = pred;
    }

    public RORecordScan open() {
        RORecordScan s = p.open();
        return new A_Select_RWRecordScan(s, pred);
    }


    public TableDefinition schema() {
        return p.schema();
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }
}
