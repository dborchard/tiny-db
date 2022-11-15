package edu.utdallas.davisbase.d_scans.impl;

import edu.utdallas.davisbase.b2_index.Index;
import edu.utdallas.davisbase.d_scans.Scan;
import edu.utdallas.davisbase.e_record.RID;
import edu.utdallas.davisbase.d_scans.domains.Constant;


public class SelectOnIndexScan implements Scan {
    private TableScan ts;
    private Index idx;
    private Constant val;


    public SelectOnIndexScan(TableScan ts, Index idx, Constant val) {
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        beforeFirst();
    }

    public void beforeFirst() {
        idx.beforeFirst(val);
    }

    public boolean next() {
        boolean ok = idx.next();
        if (ok) {
            RID rid = idx.getDataRid();
            ts.moveToRid(rid);
        }
        return ok;
    }


    public int getInt(String fldname) {
        return ts.getInt(fldname);
    }

    public String getString(String fldname) {
        return ts.getString(fldname);
    }


    public Constant getVal(String fldname) {
        return ts.getVal(fldname);
    }


    public boolean hasField(String fldname) {
        return ts.hasField(fldname);
    }


    public void close() {
        idx.close();
        ts.close();
    }
}
