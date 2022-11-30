package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.index.IndexInfo;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.d_sql_scans.SelectUsingIndexScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.iterator.heap.HeapRecordScan;
import edu.utdallas.davisbase.server.d_storage_engine.common.scans.RScan;
import edu.utdallas.davisbase.server.d_storage_engine.RWIndexScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordValueSchema;

/**
 * The Plan class corresponding to the <i>indexselect</i>
 * relational algebra operator.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
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


    public RScan open() {
        // throws an exception if p is not a tableplan.
        HeapRecordScan ts = (HeapRecordScan) p.open();
        RWIndexScan idx = ii.open();
        return new SelectUsingIndexScan(ts, idx, val);
    }

    public RecordValueSchema schema() {
        return p.schema();
    }

    @Override
    public int blocksAccessed() {
        return ii.blocksAccessed() + ii.recordsOutput();
    }
}
