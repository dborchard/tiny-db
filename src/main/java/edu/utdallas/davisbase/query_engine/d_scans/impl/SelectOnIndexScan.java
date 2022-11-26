package edu.utdallas.davisbase.query_engine.d_scans.impl;

import edu.utdallas.davisbase.storage_engine.b2_index.Index;
import edu.utdallas.davisbase.query_engine.d_scans.Scan;
import edu.utdallas.davisbase.storage_engine.e_record.RID;
import edu.utdallas.davisbase.query_engine.c_parse.domain.clause.D_Constant;


public class SelectOnIndexScan implements Scan {
    private TableScan ts;
    private Index idx;
    private D_Constant val;


    public SelectOnIndexScan(TableScan ts, Index idx, D_Constant val) {
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        seekToHead();
    }

    public void seekToHead() {
        idx.seek(val);
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


    public D_Constant getVal(String fldname) {
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
