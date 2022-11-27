package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.aggregate.common;

import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.aggregate.utils.TempTable;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.UpdateScan;

/**
 * The Plan class for the <i>materialize</i> operator.
 *
 * @author Edward Sciore
 */
public class MaterializePlan implements Plan {
    private Plan srcplan;
    private Transaction tx;

    /**
     * Create a materialize plan for the specified query.
     *
     * @param srcplan the plan of the underlying query
     * @param tx      the calling transaction
     */
    public MaterializePlan(Transaction tx, Plan srcplan) {
        this.srcplan = srcplan;
        this.tx = tx;
    }


    public Scan open() {
        RecordValueSchema sch = srcplan.schema();
        TempTable temp = new TempTable(tx, sch);
        Scan src = srcplan.open();
        UpdateScan dest = temp.open();
        while (src.next()) {
            dest.seekToHead_Insert();
            for (String fldname : sch.fields())
                dest.setVal(fldname, src.getVal(fldname));
        }
        src.close();
        dest.seekToHead_Query();
        return dest;
    }


    public int blocksAccessed() {
        // create a dummy Layout object to calculate record length
        RecordValueLayout layout = new RecordValueLayout(srcplan.schema());
        double rpb = (double) (tx.blockSize() / layout.slotSize());
        return (int) Math.ceil(srcplan.recordsOutput() / rpb);
    }


    public int recordsOutput() {
        return srcplan.recordsOutput();
    }

    /**
     * Return the number of distinct field values,
     * which is the same as in the underlying plan.
     *
     * @see simpledb.plan.Plan#distinctValues(String)
     */
    public int distinctValues(String fldname) {
        return srcplan.distinctValues(fldname);
    }

    /**
     * Return the schema of the materialized table,
     * which is the same as in the underlying plan.
     *
     * @see simpledb.plan.Plan#schema()
     */
    public RecordValueSchema schema() {
        return srcplan.schema();
    }
}
