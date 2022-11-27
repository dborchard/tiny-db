package edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl;

import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.query_engine.d_scans.Scan;
import edu.utdallas.davisbase.db.query_engine.d_scans.impl.SelectScan;
import edu.utdallas.davisbase.db.frontend.domain.clause.A_Predicate;
import edu.utdallas.davisbase.db.query_engine.e_record.Schema;

/**
 * The Plan class corresponding to the <i>select</i>
 * relational algebra operator.
 *
 * @author Edward Sciore
 */
public class SelectPlan implements Plan {
    private Plan p;
    private A_Predicate pred;

    public SelectPlan(Plan p, A_Predicate pred) {
        this.p = p;
        this.pred = pred;
    }

    public Scan open() {
        Scan s = p.open();
        return new SelectScan(s, pred);
    }


    public Schema schema() {
        return p.schema();
    }
}
