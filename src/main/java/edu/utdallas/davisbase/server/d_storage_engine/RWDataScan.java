package edu.utdallas.davisbase.server.d_storage_engine;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.common.scans.RScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordKey;

/**
 * The interface implemented by all updatable scans.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public interface RWDataScan extends RScan {
    /**
     * Modify the field value of the current record.
     *
     * @param fldname the name of the field
     * @param val     the new value, expressed as a Constant
     */
    public void setVal(String fldname, D_Constant val);

    /**
     * Modify the field value of the current record.
     *
     * @param fldname the name of the field
     * @param val     the new integer value
     */
    public void setInt(String fldname, int val);

    /**
     * Modify the field value of the current record.
     *
     * @param fldname the name of the field
     * @param val     the new string value
     */
    public void setString(String fldname, String val);

    /**
     * Insert a new record somewhere in the scan.
     */
    public void seekToHead_Insert();

    /**
     * Delete the current record from the scan.
     */
    public void delete();

    /**
     * Return the id of the current record.
     *
     * @return the id of the current record
     */
    public RecordKey getRid();

    /**
     * Position the scan so that the current record has
     * the specified id.
     *
     * @param recordKey the id of the desired record
     */
    public void moveToRid(RecordKey recordKey);
}
