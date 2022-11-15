package simpledb.b1_metadata.table;

import simpledb.d_scans.impl.TableScan;
import simpledb.e_record.Layout;
import simpledb.e_record.Schema;
import simpledb.f_tx.Transaction;

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
    private Layout tcatLayout, fcatLayout;

    /**
     * Create a new catalog manager for the database system.
     * If the database is new, the two catalog tables
     * are created.
     *
     * @param isNew has the value true if the database is new
     * @param tx    the startup transaction
     */
    public TableMgr(boolean isNew, Transaction tx) {
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField("tblname", MAX_NAME);
        tcatSchema.addIntField("slotsize");
        tcatLayout = new Layout(tcatSchema);

        Schema fcatSchema = new Schema();
        fcatSchema.addStringField("tblname", MAX_NAME);
        fcatSchema.addStringField("fldname", MAX_NAME);
        fcatSchema.addIntField("type");
        fcatSchema.addIntField("length");
        fcatSchema.addIntField("offset");
        fcatLayout = new Layout(fcatSchema);

        if (isNew) {
            createTable("davisbase_tables", tcatSchema, tx);
            createTable("davisbase_columns", fcatSchema, tx);
        }
    }

    /**
     * Create a new table having the specified name and schema.
     *
     * @param tblname the name of the new table
     * @param sch     the table's schema
     * @param tx      the transaction creating the table
     */
    public void createTable(String tblname, Schema sch, Transaction tx) {
        Layout layout = new Layout(sch);

        TableScan tcat = new TableScan(tx, "davisbase_tables", tcatLayout);
        tcat.insert();
        tcat.setString("tblname", tblname);
        tcat.setInt("slotsize", layout.slotSize());
        tcat.close();

        TableScan fcat = new TableScan(tx, "davisbase_columns", fcatLayout);
        for (String fldname : sch.fields()) {
            fcat.insert();
            fcat.setString("tblname", tblname);
            fcat.setString("fldname", fldname);
            fcat.setInt("type", sch.type(fldname));
            fcat.setInt("length", sch.length(fldname));
            fcat.setInt("offset", layout.offset(fldname));
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
    public Layout getLayout(String tblname, Transaction tx) {
        int size = -1;
        TableScan tcat = new TableScan(tx, "davisbase_tables", tcatLayout);
        while (tcat.next())
            if (tcat.getString("tblname").equals(tblname)) {
                size = tcat.getInt("slotsize");
                break;
            }
        tcat.close();

        Schema sch = new Schema();
        Map<String, Integer> offsets = new HashMap<String, Integer>();
        TableScan fcat = new TableScan(tx, "davisbase_columns", fcatLayout);
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
        return new Layout(sch, offsets, size);
    }
}