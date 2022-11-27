package edu.utdallas.davisbase.db.storage_engine.a_io.data;


import edu.utdallas.davisbase.db.storage_engine.d_file.Page;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;

/**
 * Description of the structure of a record.
 * It contains the name, type, length and offset of
 * each field of the table.
 * @author Edward Sciore
 *
 */
public class TableFileLayout {
   private TableSchema tableSchema;
   private Map<String,Integer> offsets;
   private int slotsize;

   /**
    * This constructor creates a Layout object from a schema. 
    * This constructor is used when a table 
    * is created. It determines the physical offset of 
    * each field within the record.
    * @param tblname the name of the table
    * @param tableSchema the schema of the table's records
    */
   public TableFileLayout(TableSchema tableSchema) {
      this.tableSchema = tableSchema;
      offsets  = new HashMap<>();
      int pos = Integer.BYTES; // leave space for the empty/inuse flag
      for (String fldname : tableSchema.fields()) {
         offsets.put(fldname, pos);
         pos += lengthInBytes(fldname);
      }
      slotsize = pos;
   }

   /**
    * Create a Layout object from the specified metadata.
    * This constructor is used when the metadata
    * is retrieved from the catalog.
    * @param tblname the name of the table
    * @param tableSchema the schema of the table's records
    * @param offsets the already-calculated offsets of the fields within a record
    * @param recordlen the already-calculated length of each record
    */
   public TableFileLayout(TableSchema tableSchema, Map<String,Integer> offsets, int slotsize) {
      this.tableSchema = tableSchema;
      this.offsets   = offsets;
      this.slotsize = slotsize;
   }

   /**
    * Return the schema of the table's records
    * @return the table's record schema
    */
   public TableSchema schema() {
      return tableSchema;
   }

   /**
    * Return the offset of a specified field within a record
    * @param fldname the name of the field
    * @return the offset of that field within a record
    */
   public int offset(String fldname) {
      return offsets.get(fldname);
   }

   /**
    * Return the size of a slot, in bytes.
    * @return the size of a slot
    */
   public int slotSize() {
      return slotsize;
   }

   private int lengthInBytes(String fldname) {
      int fldtype = tableSchema.type(fldname);
      if (fldtype == INTEGER)
         return Integer.BYTES;
      else
         return Page.maxBytesRequiredForString(tableSchema.length(fldname));
   }
}

