package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.aggregate.utils;

import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular.TableScan;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.UpdateScan;

/**
 * A class that creates temporary tables.
 * A temporary table is not registered in the catalog.
 * The class therefore has a method getTableInfo to return the
 * table's metadata.
 *
 * @author Edward Sciore
 */
public class TempTable {
    private static int nextTableNum = 0;
    private Transaction tx;
    private String tblname;
    private RecordValueLayout layout;

    /**
     * Allocate a name for for a new temporary table
     * having the specified schema.
     *
     * @param sch the new table's schema
     * @param tx  the calling transaction
     */
    public TempTable(Transaction tx, RecordValueSchema sch) {
        this.tx = tx;
        tblname = nextTableName();
        layout = new RecordValueLayout(sch);
    }

    private static synchronized String nextTableName() {
        nextTableNum++;
        return "temp" + nextTableNum;
    }

    /**
     * Open a table scan for the temporary table.
     */
    public UpdateScan open() {
        return new TableScan(tx, tblname, layout);
    }

    public String tableName() {
        return tblname;
    }

    /**
     * Return the table's metadata.
     *
     * @return the table's metadata
     */
    public RecordValueLayout getLayout() {
        return layout;
    }
}