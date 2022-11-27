package edu.utdallas.davisbase.db.query_engine.c_sql_scans;

import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;
import edu.utdallas.davisbase.db.storage_engine.a_scans.Scan;
import edu.utdallas.davisbase.db.storage_engine.Scan_TableScan;
import edu.utdallas.davisbase.db.storage_engine.b_io.data.heap.RecordId;
import edu.utdallas.davisbase.db.storage_engine.b_io.index.Index;


public class SelectUsingIndexScan implements Scan {
    private final Scan_TableScan ts;
    private final Index idx;

    // Start value of Index Field.
    private final D_Constant val;


    public SelectUsingIndexScan(Scan_TableScan ts, Index idx, D_Constant val) {
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        seekToHead_Query();
    }

    public void seekToHead_Query() {
        idx.seek(val);
    }

    public boolean next() {
        boolean ok = idx.next();
        if (ok) {
            RecordId recordID = idx.getRecordId();
            ts.moveToRid(recordID);
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
