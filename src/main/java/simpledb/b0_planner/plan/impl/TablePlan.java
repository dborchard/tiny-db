package simpledb.b0_planner.plan.impl;

import simpledb.b0_planner.plan.Plan;
import simpledb.b1_metadata.MetadataMgr;
import simpledb.d_scans.Scan;
import simpledb.e_record.Layout;
import simpledb.e_record.Schema;
import simpledb.d_scans.impl.TableScan;
import simpledb.f_tx.Transaction;

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
