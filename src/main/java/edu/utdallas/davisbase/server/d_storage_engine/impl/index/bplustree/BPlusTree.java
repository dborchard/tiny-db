package edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.RWIndexScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordKey;

public class BPlusTree implements RWIndexScan {
    @Override
    public void insert(D_Constant key, RecordKey value) {

    }

    @Override
    public void delete(D_Constant key, RecordKey value) {

    }

    @Override
    public void seek(D_Constant key) {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public RecordKey getRecordId() {
        return null;
    }

    @Override
    public void close() {

    }
}
