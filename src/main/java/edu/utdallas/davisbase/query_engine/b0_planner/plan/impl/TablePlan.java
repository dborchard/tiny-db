package edu.utdallas.davisbase.query_engine.b0_planner.plan.impl;

import edu.utdallas.davisbase.query_engine.b0_planner.plan.Plan;
import edu.utdallas.davisbase.query_engine.b1_metadata.MetadataMgr;
import edu.utdallas.davisbase.query_engine.d_scans.Scan;
import edu.utdallas.davisbase.query_engine.d_scans.impl.TableScan;
import edu.utdallas.davisbase.storage_engine.e_record.Layout;
import edu.utdallas.davisbase.storage_engine.e_record.Schema;
import edu.utdallas.davisbase.query_engine.f_tx.Transaction;

/**
 * The Plan class corresponding to a table.
 *
 * @author Edward Sciore
 */
public class TablePlan implements Plan {
    private String tblname;
    private Transaction tx;
    private Layout layout;


    public TablePlan(Transaction tx, String tblname, MetadataMgr md) {
        this.tblname = tblname;
        this.tx = tx;
        layout = md.getLayout(tblname, tx);
    }

    public Scan open() {
        return new TableScan(tx, tblname, layout);
    }


    public Schema schema() {
        return layout.schema();
    }
}
