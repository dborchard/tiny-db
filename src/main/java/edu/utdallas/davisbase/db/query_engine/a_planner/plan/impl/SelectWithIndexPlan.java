package edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl;

import edu.utdallas.davisbase.db.query_engine.e_record.Schema;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.query_engine.d_scans.Scan;
import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;
import edu.utdallas.davisbase.db.query_engine.d_scans.impl.SelectOnIndexScan;
import edu.utdallas.davisbase.db.query_engine.d_scans.impl.TableScan;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.Index;
import edu.utdallas.davisbase.db.query_engine.b_metadata.index.IndexInfo;

/**
 * The Plan class corresponding to the <i>indexselect</i>
 * relational algebra operator.
 *
 * @author Edward Sciore
 */
public class SelectWithIndexPlan implements Plan {
    private Plan p;
    private IndexInfo ii;
    private D_Constant val;


    public SelectWithIndexPlan(Plan p, IndexInfo ii, D_Constant val) {
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
