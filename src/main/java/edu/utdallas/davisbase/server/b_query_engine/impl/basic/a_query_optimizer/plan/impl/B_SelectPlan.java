package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.c_sql_scans.A_Select_RWRecordScan;
import edu.utdallas.davisbase.server.d_storage_engine.RORecordScan;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.table.TableDefinition;

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
