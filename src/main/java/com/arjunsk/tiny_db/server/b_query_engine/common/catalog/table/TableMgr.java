package com.arjunsk.tiny_db.server.b_query_engine.common.catalog.table;

import com.arjunsk.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import com.arjunsk.tiny_db.server.d_storage_engine.impl.data.heap.HeapRWRecordScan;

import java.util.HashMap;
import java.util.Map;

/**
 * The table manager. There are methods to create a table, save the metadata in the catalog, and
 * obtain the metadata of a previously-created table.
 *
 * @author Edward Sciore
 */
public class TableMgr {

  // The max characters a tablename or fieldname can have.
  public static final int MAX_NAME = 16;
  private TablePhysicalLayout tcatRecordValueLayout, fcatRecordValueLayout;

  /**
   * Create a new catalog manager for the database system. If the database is new, the two catalog
   * tables are created.
   *
   * @param isNew has the value true if the database is new
   * @param tx    the startup transaction
   */
  public TableMgr(boolean isNew, Transaction tx) {
    TableDefinition tcatTableDefinition = new TableDefinition();
    tcatTableDefinition.addStringField("tblname", MAX_NAME);
    tcatTableDefinition.addIntField("slotsize");
    tcatRecordValueLayout = new TablePhysicalLayout(tcatTableDefinition);

    TableDefinition fcatTableDefinition = new TableDefinition();
    fcatTableDefinition.addStringField("tblname", MAX_NAME);
    fcatTableDefinition.addStringField("fldname", MAX_NAME);
    fcatTableDefinition.addIntField("type");
    fcatTableDefinition.addIntField("length");
    fcatTableDefinition.addIntField("offset");
    fcatRecordValueLayout = new TablePhysicalLayout(fcatTableDefinition);

    if (isNew) {
      createTable("tinydb_tables", tcatTableDefinition, tx);
      createTable("tinydb_columns", fcatTableDefinition, tx);
    }
  }

  /**
   * Create a new table having the specified name and schema.
   *
   * @param tblname the name of the new table
   * @param sch     the table's schema
   * @param tx      the transaction creating the table
   */
  public void createTable(String tblname, TableDefinition sch, Transaction tx) {
    TablePhysicalLayout recordValueLayout = new TablePhysicalLayout(sch);

    HeapRWRecordScan tcat = new HeapRWRecordScan(tx, "tinydb_tables", tcatRecordValueLayout);
    tcat.seekToInsertStart();
    tcat.setString("tblname", tblname);
    tcat.setInt("slotsize", recordValueLayout.slotSize());
    tcat.close();

    HeapRWRecordScan fcat = new HeapRWRecordScan(tx, "tinydb_columns", fcatRecordValueLayout);
    for (String fldname : sch.fields()) {
      fcat.seekToInsertStart();
      fcat.setString("tblname", tblname);
      fcat.setString("fldname", fldname);
      fcat.setInt("type", sch.type(fldname));
      fcat.setInt("length", sch.length(fldname));
      fcat.setInt("offset", recordValueLayout.offset(fldname));
    }
    fcat.close();
  }

  /**
   * Retrieve the layout of the specified table from the catalog.
   *
   * @param tblname the name of the table
   * @param tx      the transaction
   * @return the table's stored metadata
   */
  public TablePhysicalLayout getLayout(String tblname, Transaction tx) {
    int size = -1;
    HeapRWRecordScan tcat = new HeapRWRecordScan(tx, "tinydb_tables", tcatRecordValueLayout);
    // [tableName1, slotsize1] | [tableName2, slotsize2] | [tableName3, slotsize3]
    while (tcat.next()) {
      if (tcat.getString("tblname").equals(tblname)) {
        size = tcat.getInt("slotsize");
        break;
      }
    }
    tcat.close();

    TableDefinition sch = new TableDefinition();
    Map<String, Integer> offsets = new HashMap<String, Integer>();
    HeapRWRecordScan fcat = new HeapRWRecordScan(tx, "tinydb_columns", fcatRecordValueLayout);
    // [tableName1, A, int, 4, offset] | [tableName1, B, varchar, 9, offset] | [tableName3, fldname, type, length offset]
    while (fcat.next()) {
      if (fcat.getString("tblname").equals(tblname)) {
        String fldname = fcat.getString("fldname");
        int fldtype = fcat.getInt("type");
        int fldlen = fcat.getInt("length");
        int offset = fcat.getInt("offset");
        offsets.put(fldname, offset);
        sch.addField(fldname, fldtype, fldlen);
      }
    }
    fcat.close();
    return new TablePhysicalLayout(sch, offsets, size);
  }
}