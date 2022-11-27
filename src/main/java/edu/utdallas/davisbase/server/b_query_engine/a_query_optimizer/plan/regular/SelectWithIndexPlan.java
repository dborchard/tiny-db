package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.regular;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.index.IndexInfo;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular.SelectUsingIndexScan;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular.TableScan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.b_index.Index;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;

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
        return new SelectUsingIndexScan(ts, idx, val);
    }

    public RecordValueSchema schema() {
        return p.schema();
    }

    /**
     * Estimates the number of block accesses to compute the
     * index selection, which is the same as the
     * index traversal cost plus the number of matching data records.
     *
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return ii.blocksAccessed() + recordsOutput();
    }

    /**
     * Estimates the number of output records in the index selection,
     * which is the same as the number of search key values
     * for the index.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return ii.recordsOutput();
    }

    /**
     * Returns the distinct values as defined by the index.
     *
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return ii.distinctValues(fldname);
    }

}
