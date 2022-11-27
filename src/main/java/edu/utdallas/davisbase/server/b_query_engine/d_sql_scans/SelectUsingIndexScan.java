package edu.utdallas.davisbase.server.b_query_engine.d_sql_scans;

import edu.utdallas.davisbase.server.a_frontend.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.TableDataScan;
import edu.utdallas.davisbase.server.d_storage_engine.common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.index.Index;
import edu.utdallas.davisbase.server.d_storage_engine.file_organization.heap.RecordId;


public class SelectUsingIndexScan implements Scan {
    private final TableDataScan ts;
    private final Index idx;

    // Start value of Index Field.
    private final D_Constant val;


    public SelectUsingIndexScan(TableDataScan ts, Index idx, D_Constant val) {
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
