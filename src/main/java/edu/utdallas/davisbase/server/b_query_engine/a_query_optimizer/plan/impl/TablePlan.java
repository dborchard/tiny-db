package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.TableScan;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_file_organization.heap.RecordValueSchema;

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
}
