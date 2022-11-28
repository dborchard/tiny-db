package edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.TableRowScan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordKey;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.b_index.Index;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;


public class SelectUsingIndexScan implements Scan {
    private final TableRowScan ts;
    private final Index idx;

    // Start value of Index Field.
    private final D_Constant val;


    public SelectUsingIndexScan(TableRowScan ts, Index idx, D_Constant val) {
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
            RecordKey recordKey = idx.getRecordId();
            ts.moveToRid(recordKey);
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
