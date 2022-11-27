package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.regular;

import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular.TableScan;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;

/**
 * The Plan class corresponding to a table.
 *
 * @author Edward Sciore
 */
public class TablePlan implements Plan {
    private String tblname;
    private Transaction tx;
    private RecordValueLayout recordValueLayout;
    private StatInfo si;

    public TablePlan(Transaction tx, String tblname, MetadataMgr md) {
        this.tblname = tblname;
        this.tx = tx;
        recordValueLayout = md.getLayout(tblname, tx);
    }

    public Scan open() {
        return new TableScan(tx, tblname, recordValueLayout);
    }


    public RecordValueSchema schema() {
        return recordValueLayout.schema();
    }

    @Override
    public int blocksAccessed() {
        return si.blocksAccessed();
    }

    /**
     * Estimates the number of records in the table,
     * which is obtainable from the statistics manager.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return si.recordsOutput();
    }

    /**
     * Estimates the number of distinct field values in the table,
     * which is obtainable from the statistics manager.
     *
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return si.distinctValues(fldname);
    }

}
