package edu.utdallas.davisbase.db.query_engine.c_scans;

import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.RecordId;
import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;

/**
 * The interface implemented by all updateable scans.
 * @author Edward Sciore
 */
public interface UpdateScan extends Scan {
   /**
    * Modify the field value of the current record.
    * @param fldname the name of the field
    * @param val the new value, expressed as a Constant
    */
   public void setVal(String fldname, D_Constant val);
   
   /**
    * Modify the field value of the current record.
    * @param fldname the name of the field
    * @param val the new integer value
    */
   public void setInt(String fldname, int val);
   
   /**
    * Modify the field value of the current record.
    * @param fldname the name of the field
    * @param val the new string value
    */
   public void setString(String fldname, String val);
   
   /**
    * Insert a new record somewhere in the scan.
    */
   public void seekToHead_Update();
   
   /**
    * Delete the current record from the scan.
    */
   public void delete();
   
   /**
    * Return the id of the current record.
    * @return the id of the current record
    */
   public RecordId getRid();
   
   /**
    * Position the scan so that the current record has
    * the specified id.
    * @param recordID the id of the desired record
    */
   public void moveToRid(RecordId recordID);
}
