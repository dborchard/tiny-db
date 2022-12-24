package edu.utdallas.tiny_db.server.b_query_engine.impl.basic.b_execution_engine;

import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.tiny_db.server.d_storage_engine.RORecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.RWIndexScan;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.HeapRWRecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.page.RecordKey;


/**
 * The Scan using Index. (Invoked by QueryPlanner)
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class A_SelectUsingIndex_RORecordScan implements RORecordScan {
    private final HeapRWRecordScan ts;
    private final RWIndexScan idx;

    // Start value of Index Field.
    private final D_Constant val;


    public A_SelectUsingIndex_RORecordScan(HeapRWRecordScan ts, RWIndexScan idx, D_Constant val) {
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        seekToQueryStart();
    }

    public void seekToQueryStart() {
        idx.seek(val);
    }

    public boolean next() {
        boolean ok = idx.hasNext();
        if (ok) {
            RecordKey recordKey = idx.next();
            ts.seekTo(recordKey);
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
