package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.index.IndexInfo;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.SelectUsingIndexScan;
import edu.utdallas.davisbase.server.d_storage_engine.TableDataScan;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.b_index.Index;
import edu.utdallas.davisbase.server.d_storage_engine.a_file_organization.heap.TableSchema;

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
        TableDataScan ts = (TableDataScan) p.open();
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
