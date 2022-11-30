package edu.utdallas.davisbase.server.b_query_engine.impl.basic.d_sql_scans;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.iterator.heap.HeapRecordScan;
import edu.utdallas.davisbase.server.d_storage_engine.common.scans.RScan;
import edu.utdallas.davisbase.server.d_storage_engine.RWIndexScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordKey;


/**
 * The Scan using Index. (Invoked by QueryPlanner)
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class SelectUsingIndexScan implements RScan {
    private final HeapRecordScan ts;
    private final RWIndexScan idx;

    // Start value of Index Field.
    private final D_Constant val;


    public SelectUsingIndexScan(HeapRecordScan ts, RWIndexScan idx, D_Constant val) {
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        seekToHead_Query();
    }

    public void seekToHead_Query() {
        idx.seek(val);
    }

    public boolean next() {
        boolean ok = idx.hasNext();
        if (ok) {
            RecordKey recordKey = idx.next();
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
