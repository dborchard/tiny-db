package simpledb.b0_planner.plan.impl;

import simpledb.b0_planner.plan.Plan;
import simpledb.d_scans.Scan;
import simpledb.d_scans.domains.Constant;
import simpledb.d_scans.impl.SelectOnIndexScan;
import simpledb.d_scans.impl.TableScan;
import simpledb.e_record.Schema;
import simpledb.b2_index.Index;
import simpledb.b1_metadata.index.IndexInfo;

/**
 * The Plan class corresponding to the <i>indexselect</i>
 * relational algebra operator.
 *
 * @author Edward Sciore
 */
public class SelectWithIndexPlan implements Plan {
    private Plan p;
    private IndexInfo ii;
    private Constant val;


    public SelectWithIndexPlan(Plan p, IndexInfo ii, Constant val) {
        this.p = p;
        this.ii = ii;
        this.val = val;
    }


    public Scan open() {
        // throws an exception if p is not a tableplan.
        TableScan ts = (TableScan) p.open();
        Index idx = ii.open();
        return new SelectOnIndexScan(ts, idx, val);
    }

    public Schema schema() {
        return p.schema();
    }
}
