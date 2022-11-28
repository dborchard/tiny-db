package edu.utdallas.davisbase.server.d_storage_engine.a_disk.b_index.hash;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.TableRowScan;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordKey;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.b_index.Index;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 *
 * @author Edward Sciore
 */
public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;
    private Transaction tx;
    private String idxname;
    private RecordValueLayout layout;
    private D_Constant searchkey = null;
    private TableRowScan ts = null;


    public HashIndex(Transaction tx, String idxname, RecordValueLayout layout) {
        this.tx = tx;
        this.idxname = idxname;
        this.layout = layout;
    }

    public static int searchCost(int numblocks, int rpb) {
        return numblocks / HashIndex.NUM_BUCKETS;
    }

    public void seek(D_Constant key) {
        close();
        this.searchkey = key;
        int bucket = key.hashCode() % NUM_BUCKETS;
        String tblname = idxname + bucket;
        ts = new TableRowScan(tx, tblname, layout);
    }

    public boolean next() {
        while (ts.next()) if (ts.getVal("dataval").equals(searchkey)) return true;
        return false;
    }

    public RecordKey getRecordId() {
        int blknum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RecordKey(blknum, id);
    }

    public void insert(D_Constant key, RecordKey value) {
        seek(key);
        ts.seekToHead_Insert();
        ts.setInt("block", value.blockNumber());
        ts.setInt("id", value.slot());
        ts.setVal("dataval", key);
    }

    public void delete(D_Constant key, RecordKey value) {
        seek(key);
        while (next()) if (getRecordId().equals(value)) {
            ts.delete();
            return;
        }
    }

    public void close() {
        if (ts != null) ts.close();
    }
}
