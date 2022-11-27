package edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.query_engine.d_domain.misc.StatInfo;
import edu.utdallas.davisbase.server.query_engine.b_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.storage_engine.Scan_TableScan;
import edu.utdallas.davisbase.server.storage_engine.Transaction;
import edu.utdallas.davisbase.server.storage_engine.a_scans.Scan;
import edu.utdallas.davisbase.server.storage_engine.b_io.data.heap.TableFileLayout;
import edu.utdallas.davisbase.server.storage_engine.b_io.data.heap.TableSchema;

/**
 * The Plan class corresponding to a table.
 *
 * @author Edward Sciore
 */
public class TablePlan implements Plan {
    private String tblname;
    private Transaction tx;
    private TableFileLayout tableFileLayout;
    private StatInfo si;

    public TablePlan(Transaction tx, String tblname, MetadataMgr md) {
        this.tblname = tblname;
        this.tx = tx;
        tableFileLayout = md.getLayout(tblname, tx);
    }

    public Scan open() {
        return new Scan_TableScan(tx, tblname, tableFileLayout);
    }


    public TableSchema schema() {
        return tableFileLayout.schema();
    }

    @Override
    public int blocksAccessed() {
        return si.blocksAccessed();
    }
}
