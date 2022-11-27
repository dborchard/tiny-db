package edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl;

import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.query_engine.b_metadata.MetadataMgr;
import edu.utdallas.davisbase.db.storage_engine.a_scans.Scan;
import edu.utdallas.davisbase.db.storage_engine.Scan_TableScan;
import edu.utdallas.davisbase.db.storage_engine.b_io.data.heap.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.b_io.data.heap.TableSchema;
import edu.utdallas.davisbase.db.storage_engine.Transaction;

/**
 * The Plan class corresponding to a table.
 *
 * @author Edward Sciore
 */
public class TablePlan implements Plan {
    private String tblname;
    private Transaction tx;
    private TableFileLayout tableFileLayout;


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
}
