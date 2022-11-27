package edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.frontend.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.query_engine.b_metadata.index.IndexInfo;
import edu.utdallas.davisbase.server.storage_engine.a_scans.Scan;
import edu.utdallas.davisbase.server.query_engine.c_sql_scans.SelectUsingIndexScan;
import edu.utdallas.davisbase.server.storage_engine.Scan_TableScan;
import edu.utdallas.davisbase.server.storage_engine.b_io.data.heap.TableSchema;
import edu.utdallas.davisbase.server.storage_engine.b_io.index.Index;

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
        Scan_TableScan ts = (Scan_TableScan) p.open();
        Index idx = ii.open();
        return new SelectUsingIndexScan(ts, idx, val);
    }

    public TableSchema schema() {
        return p.schema();
    }

    @Override
    public int blocksAccessed() {
        return ii.blocksAccessed() + ii.recordsOutput();
    }
}
