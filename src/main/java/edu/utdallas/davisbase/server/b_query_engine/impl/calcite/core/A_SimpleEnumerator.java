package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.core;

import edu.utdallas.davisbase.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.RWDataScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.iterator.heap.HeapRecordScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordValueLayout;
import org.apache.calcite.linq4j.Enumerator;

class A_SimpleEnumerator implements Enumerator<Object[]> {

    private final RWDataScan scan;
    private final RecordValueLayout layout;

    A_SimpleEnumerator(MetadataMgr mdm, Transaction tx, String tableName) {
        layout = mdm.getLayout(tableName, tx);
        this.scan = new HeapRecordScan(tx, tableName, layout);
    }

    @Override
    public Object[] current() {
        return layout.schema().fields().stream().map(this.scan::getVal).map(e -> e.asInt() != null ? e.asInt() : e.asString()).toArray();
    }

    @Override
    public boolean moveNext() {
        return this.scan.next();
    }

    @Override
    public void reset() {
        this.scan.seekToHead_Query();
    }

    @Override
    public void close() {
        this.scan.close();
    }

}