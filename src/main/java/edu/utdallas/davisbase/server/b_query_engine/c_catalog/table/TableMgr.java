package edu.utdallas.davisbase.server.b_query_engine.c_catalog.table;

import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.TableScan;
import edu.utdallas.davisbase.server.d_storage_engine.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_file_organization.heap.RecordValueSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * The table manager.
 * There are methods to create a table, save the metadata
 * in the catalog, and obtain the metadata of a
 * previously-created table.
 *
 * @author Edward Sciore
 */
public class TableMgr {
    // The max characters a tablename or fieldname can have.
    public static final int MAX_NAME = 16;
    private RecordValueLayout tcatRecordValueLayout, fcatRecordValueLayout;

    /**
     * Create a new catalog manager for the database system.
     * If the database is new, the two catalog tables
     * are created.
     *
     * @param isNew has the value true if the database is new
     * @param tx    the startup transaction
     */
    public TableMgr(boolean isNew, Transaction tx) {
        RecordValueSchema tcatRecordValueSchema = new RecordValueSchema();
        tcatRecordValueSchema.addStringField("tblname", MAX_NAME);
        tcatRecordValueSchema.addIntField("slotsize");
        tcatRecordValueLayout = new RecordValueLayout(tcatRecordValueSchema);

        RecordValueSchema fcatRecordValueSchema = new RecordValueSchema();
        fcatRecordValueSchema.addStringField("tblname", MAX_NAME);
        fcatRecordValueSchema.addStringField("fldname", MAX_NAME);
        fcatRecordValueSchema.addIntField("type");
        fcatRecordValueSchema.addIntField("length");
        fcatRecordValueSchema.addIntField("offset");
        fcatRecordValueLayout = new RecordValueLayout(fcatRecordValueSchema);

        if (isNew) {
            createTable("davisbase_tables", tcatRecordValueSchema, tx);
            createTable("davisbase_columns", fcatRecordValueSchema, tx);
        }
    }

    /**
     * Create a new table having the specified name and schema.
     *
     * @param tblname the name of the new table
     * @param sch     the table's schema
     * @param tx      the transaction creating the table
     */
    public void createTable(String tblname, RecordValueSchema sch, Transaction tx) {
        RecordValueLayout recordValueLayout = new RecordValueLayout(sch);

        TableScan tcat = new TableScan(tx, "davisbase_tables", tcatRecordValueLayout);
        tcat.seekToHead_Insert();
        tcat.setString("tblname", tblname);
        tcat.setInt("slotsize", recordValueLayout.slotSize());
        tcat.close();

        TableScan fcat = new TableScan(tx, "davisbase_columns", fcatRecordValueLayout);
        for (String fldname : sch.fields()) {
            fcat.seekToHead_Insert();
            fcat.setString("tblname", tblname);
            fcat.setString("fldname", fldname);
            fcat.setInt("type", sch.type(fldname));
            fcat.setInt("length", sch.length(fldname));
            fcat.setInt("offset", recordValueLayout.offset(fldname));
        }
        fcat.close();
    }

    /**
     * Retrieve the layout of the specified table
     * from the catalog.
     *
     * @param tblname the name of the table
     * @param tx      the transaction
     * @return the table's stored metadata
     */
    public RecordValueLayout getLayout(String tblname, Transaction tx) {
        int size = -1;
        TableScan tcat = new TableScan(tx, "davisbase_tables", tcatRecordValueLayout);
        while (tcat.next()) if (tcat.getString("tblname").equals(tblname)) {
            size = tcat.getInt("slotsize");
            break;
        }
        tcat.close();

        RecordValueSchema sch = new RecordValueSchema();
        Map<String, Integer> offsets = new HashMap<String, Integer>();
        TableScan fcat = new TableScan(tx, "davisbase_columns", fcatRecordValueLayout);
        while (fcat.next()) if (fcat.getString("tblname").equals(tblname)) {
            String fldname = fcat.getString("fldname");
            int fldtype = fcat.getInt("type");
            int fldlen = fcat.getInt("length");
            int offset = fcat.getInt("offset");
            offsets.put(fldname, offset);
            sch.addField(fldname, fldtype, fldlen);
        }
        fcat.close();
        return new RecordValueLayout(sch, offsets, size);
    }
}