package edu.utdallas.davisbase.server.c_key_value_store;

import edu.utdallas.davisbase.server.b_query_engine.SimpleDB;
import edu.utdallas.davisbase.server.d_storage_engine.LogMgr;
import edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr.BufferMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.FileMgr;

public class TxTest {
    public static void main(String[] args) throws Exception {
        SimpleDB db = new SimpleDB("txtest", 400, 8);
        FileMgr fm = db.fileMgr();
        LogMgr lm = db.logMgr();

        BufferMgr bm = db.bufferMgr();

        Transaction tx1 = new Transaction(fm, lm, bm);
        BlockId blk = new BlockId("testfile", 1);
        tx1.pin(blk);
        // The block initially contains unknown bytes,
        // so don't log those values here.
        tx1.setInt(blk, 80, 1, false);
        tx1.setString(blk, 40, "one", false);
        tx1.commit();

        Transaction tx2 = new Transaction(fm, lm, bm);
        tx2.pin(blk);
        int ival = tx2.getInt(blk, 80);
        String sval = tx2.getString(blk, 40);
        System.out.println("initial value at location 80 = " + ival);
        System.out.println("initial value at location 40 = " + sval);
        int newival = ival + 1;
        String newsval = sval + "!";
        tx2.setInt(blk, 80, newival, true);
        tx2.setString(blk, 40, newsval, true);
        tx2.commit();
        Transaction tx3 = new Transaction(fm, lm, bm);
        tx3.pin(blk);
        System.out.println("new value at location 80 = " + tx3.getInt(blk, 80));
        System.out.println("new value at location 40 = " + tx3.getString(blk, 40));
        tx3.setInt(blk, 80, 9999, true);
        System.out.println("pre-rollback value at location 80 = " + tx3.getInt(blk, 80));
        tx3.rollback();

        Transaction tx4 = new Transaction(fm, lm, bm);
        tx4.pin(blk);
        System.out.println("post-rollback at location 80 = " + tx4.getInt(blk, 80));
        tx4.commit();
    }
}
