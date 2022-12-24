package edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.index.IndexInfo;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.b_execution_engine.A_SelectUsingIndex_RORecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.RORecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.RWIndexScan;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.HeapRWRecordScan;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;

/**
 * The Plan class corresponding to the <i>indexselect</i>
 * relational algebra operator.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class B_SelectWithIndexPlan implements Plan {
    private Plan p;
    private IndexInfo ii;
    private D_Constant val;


    public B_SelectWithIndexPlan(Plan p, IndexInfo ii, D_Constant val) {
        this.p = p;
        this.ii = ii;
        this.val = val;
    }


    public RORecordScan open() {
        // throws an exception if p is not a tableplan.
        HeapRWRecordScan scan = (HeapRWRecordScan) p.open();
        RWIndexScan idx = ii.open();
        return new A_SelectUsingIndex_RORecordScan(scan, idx, val);
    }

    public TableDefinition schema() {
        return p.schema();
    }

    @Override
    public int blocksAccessed() {
        return ii.blocksAccessed() + ii.recordsOutput();
    }
}
