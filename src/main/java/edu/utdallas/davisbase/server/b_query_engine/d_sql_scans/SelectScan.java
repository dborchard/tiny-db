package edu.utdallas.davisbase.server.b_query_engine.d_sql_scans;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.UpdateScan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordKey;

/**
 * The scan class corresponding to the <i>select</i> relational
 * algebra operator.
 * All methods except next delegate their work to the
 * underlying scan.
 *
 * @author Edward Sciore
 */
public class SelectScan implements UpdateScan {
    private Scan s;
    private A_Predicate pred;

    /**
     * Create a select scan having the specified underlying
     * scan and predicate.
     *
     * @param s    the scan of the underlying query
     * @param pred the selection predicate
     */
    public SelectScan(Scan s, A_Predicate pred) {
        this.s = s;
        this.pred = pred;
    }

    // Scan methods

    public void seekToHead_Query() {
        s.seekToHead_Query();
    }

    public boolean next() {
        while (s.next()) {
            if (pred.isSatisfied(s)) return true;
        }
        return false;
    }

    public int getInt(String fldname) {
        return s.getInt(fldname);
    }

    public String getString(String fldname) {
        return s.getString(fldname);
    }

    public D_Constant getVal(String fldname) {
        return s.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return s.hasField(fldname);
    }

    public void close() {
        s.close();
    }

    // UpdateScan methods

    public void setInt(String fldname, int val) {
        UpdateScan us = (UpdateScan) s;
        us.setInt(fldname, val);
    }

    public void setString(String fldname, String val) {
        UpdateScan us = (UpdateScan) s;
        us.setString(fldname, val);
    }

    public void setVal(String fldname, D_Constant val) {
        UpdateScan us = (UpdateScan) s;
        us.setVal(fldname, val);
    }

    public void delete() {
        UpdateScan us = (UpdateScan) s;
        us.delete();
    }

    public void seekToHead_Insert() {
        UpdateScan us = (UpdateScan) s;
        us.seekToHead_Insert();
    }

    public RecordKey getRid() {
        UpdateScan us = (UpdateScan) s;
        return us.getRid();
    }

    public void moveToRid(RecordKey recordKey) {
        UpdateScan us = (UpdateScan) s;
        us.moveToRid(recordKey);
    }
}
