package edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl;

import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.query_engine.b_metadata.MetadataMgr;
import edu.utdallas.davisbase.db.query_engine.c_scans.Scan;
import edu.utdallas.davisbase.db.query_engine.c_scans.impl.TableScan;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.TableSchema;
import edu.utdallas.davisbase.db.storage_engine.b_transaction.Transaction;

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
        return new TableScan(tx, tblname, tableFileLayout);
    }


    public TableSchema schema() {
        return tableFileLayout.schema();
    }
}
