package simpledb.b0_planner.plan.impl;

import simpledb.b0_planner.plan.Plan;
import simpledb.d_scans.Scan;
import simpledb.d_scans.impl.SelectScan;
import simpledb.d_scans.domains.Predicate;
import simpledb.e_record.Schema;

/**
 * The Plan class corresponding to the <i>select</i>
 * relational algebra operator.
 *
 * @author Edward Sciore
 */
public class SelectPlan implements Plan {
    private Plan p;
    private Predicate pred;

    public SelectPlan(Plan p, Predicate pred) {
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
