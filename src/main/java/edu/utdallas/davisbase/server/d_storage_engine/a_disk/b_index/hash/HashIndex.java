package edu.utdallas.davisbase.server.d_storage_engine.a_disk.b_index.hash;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.TableScan;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordId;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.TableFileLayout;
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
    private TableFileLayout layout;
    private D_Constant searchkey = null;
    private TableScan ts = null;


    public HashIndex(Transaction tx, String idxname, TableFileLayout layout) {
        this.tx = tx;
        this.idxname = idxname;
        this.layout = layout;
    }


    public void seek(D_Constant searchkey) {
        close();
        this.searchkey = searchkey;
        int bucket = searchkey.hashCode() % NUM_BUCKETS;
        String tblname = idxname + bucket;
        ts = new TableScan(tx, tblname, layout);
    }


    public boolean next() {
        while (ts.next()) if (ts.getVal("dataval").equals(searchkey)) return true;
        return false;
    }


    public RecordId getRecordId() {
        int blknum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RecordId(blknum, id);
    }


    public void insert(D_Constant val, RecordId rid) {
        seek(val);
        ts.seekToHead_Insert();
        ts.setInt("block", rid.blockNumber());
        ts.setInt("id", rid.slot());
        ts.setVal("dataval", val);
    }


    public void delete(D_Constant val, RecordId rid) {
        seek(val);
        while (next()) if (getRecordId().equals(rid)) {
            ts.delete();
            return;
        }
    }

    public void close() {
        if (ts != null) ts.close();
    }


    public static int searchCost(int numblocks, int rpb) {
        return numblocks / HashIndex.NUM_BUCKETS;
    }
}
