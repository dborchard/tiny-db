package edu.utdallas.davisbase.b0_planner.plan.impl;

import edu.utdallas.davisbase.b0_planner.plan.Plan;
import edu.utdallas.davisbase.d_scans.Scan;
import edu.utdallas.davisbase.d_scans.impl.SelectScan;
import edu.utdallas.davisbase.c_parse.domain.clause.A_Predicate;
import edu.utdallas.davisbase.e_record.Schema;

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
