package edu.utdallas.davisbase.db.query_engine.b_metadata.table;

import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableSchema;
import edu.utdallas.davisbase.db.storage_engine.Transaction;
import edu.utdallas.davisbase.db.query_engine.c_scans.impl.TableScan;

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
    private TableFileLayout tcatTableFileLayout, fcatTableFileLayout;

    /**
     * Create a new catalog manager for the database system.
     * If the database is new, the two catalog tables
     * are created.
     *
     * @param isNew has the value true if the database is new
     * @param tx    the startup transaction
     */
    public TableMgr(boolean isNew, Transaction tx) {
        TableSchema tcatTableSchema = new TableSchema();
        tcatTableSchema.addStringField("tblname", MAX_NAME);
        tcatTableSchema.addIntField("slotsize");
        tcatTableFileLayout = new TableFileLayout(tcatTableSchema);

        TableSchema fcatTableSchema = new TableSchema();
        fcatTableSchema.addStringField("tblname", MAX_NAME);
        fcatTableSchema.addStringField("fldname", MAX_NAME);
        fcatTableSchema.addIntField("type");
        fcatTableSchema.addIntField("length");
        fcatTableSchema.addIntField("offset");
        fcatTableFileLayout = new TableFileLayout(fcatTableSchema);

        if (isNew) {
            createTable("davisbase_tables", tcatTableSchema, tx);
            createTable("davisbase_columns", fcatTableSchema, tx);
        }
    }

    /**
     * Create a new table having the specified name and schema.
     *
     * @param tblname the name of the new table
     * @param sch     the table's schema
     * @param tx      the transaction creating the table
     */
    public void createTable(String tblname, TableSchema sch, Transaction tx) {
        TableFileLayout tableFileLayout = new TableFileLayout(sch);

        TableScan tcat = new TableScan(tx, "davisbase_tables", tcatTableFileLayout);
        tcat.seekToHead_Update();
        tcat.setString("tblname", tblname);
        tcat.setInt("slotsize", tableFileLayout.slotSize());
        tcat.close();

        TableScan fcat = new TableScan(tx, "davisbase_columns", fcatTableFileLayout);
        for (String fldname : sch.fields()) {
            fcat.seekToHead_Update();
            fcat.setString("tblname", tblname);
            fcat.setString("fldname", fldname);
            fcat.setInt("type", sch.type(fldname));
            fcat.setInt("length", sch.length(fldname));
            fcat.setInt("offset", tableFileLayout.offset(fldname));
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
    public TableFileLayout getLayout(String tblname, Transaction tx) {
        int size = -1;
        TableScan tcat = new TableScan(tx, "davisbase_tables", tcatTableFileLayout);
        while (tcat.next())
            if (tcat.getString("tblname").equals(tblname)) {
                size = tcat.getInt("slotsize");
                break;
            }
        tcat.close();

        TableSchema sch = new TableSchema();
        Map<String, Integer> offsets = new HashMap<String, Integer>();
        TableScan fcat = new TableScan(tx, "davisbase_columns", fcatTableFileLayout);
        while (fcat.next())
            if (fcat.getString("tblname").equals(tblname)) {
                String fldname = fcat.getString("fldname");
                int fldtype = fcat.getInt("type");
                int fldlen = fcat.getInt("length");
                int offset = fcat.getInt("offset");
                offsets.put(fldname, offset);
                sch.addField(fldname, fldtype, fldlen);
            }
        fcat.close();
        return new TableFileLayout(sch, offsets, size);
    }
}