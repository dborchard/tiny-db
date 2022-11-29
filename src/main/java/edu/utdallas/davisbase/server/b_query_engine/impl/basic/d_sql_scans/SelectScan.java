package edu.utdallas.davisbase.server.b_query_engine.impl.basic.d_sql_scans;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.common.scans.RScan;
import edu.utdallas.davisbase.server.d_storage_engine.RWDataScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordKey;

/**
 * The scan class corresponding to the <i>select</i> relational
 * algebra operator.
 * All methods except next delegate their work to the
 * underlying scan.
 *
 * @author Edward Sciore
 */
public class SelectScan implements RWDataScan {
    private RScan s;
    private A_Predicate pred;

    /**
     * Create a select scan having the specified underlying
     * scan and predicate.
     *
     * @param s    the scan of the underlying query
     * @param pred the selection predicate
     */
    public SelectScan(RScan s, A_Predicate pred) {
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
        RWDataScan us = (RWDataScan) s;
        us.setInt(fldname, val);
    }

    public void setString(String fldname, String val) {
        RWDataScan us = (RWDataScan) s;
        us.setString(fldname, val);
    }

    public void setVal(String fldname, D_Constant val) {
        RWDataScan us = (RWDataScan) s;
        us.setVal(fldname, val);
    }

    public void delete() {
        RWDataScan us = (RWDataScan) s;
        us.delete();
    }

    public void seekToHead_Insert() {
        RWDataScan us = (RWDataScan) s;
        us.seekToHead_Insert();
    }

    public RecordKey getRid() {
        RWDataScan us = (RWDataScan) s;
        return us.getRid();
    }

    public void moveToRid(RecordKey recordKey) {
        RWDataScan us = (RWDataScan) s;
        us.moveToRid(recordKey);
    }
}
