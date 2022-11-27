package edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr;

import edu.utdallas.davisbase.server.b_query_engine.SimpleDB;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.c_key_value_store.b_buffer_mgr.BufferMgr;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.b_file.FileMgr;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.b_file.Page;

public class RecoveryTest {
    public static FileMgr fm;
    public static BufferMgr bm;
    private static SimpleDB db;
    private static BlockId blk0, blk1;

    public static void main(String[] args) throws Exception {
        db = new SimpleDB("recoverytest", 400, 8);
        fm = db.fileMgr();
        bm = db.bufferMgr();
        blk0 = new BlockId("testfile", 0);
        blk1 = new BlockId("testfile", 1);

        if (fm.length("testfile") == 0) {
            initialize();
            modify();
        } else {
            recover();
        }
    }

    private static void initialize() {
        Transaction tx1 = db.newTx();
        Transaction tx2 = db.newTx();
        tx1.pin(blk0);
        tx2.pin(blk1);
        int pos = 0;
        for (int i = 0; i < 6; i++) {
            tx1.setInt(blk0, pos, pos, false);
            tx2.setInt(blk1, pos, pos, false);
            pos += Integer.BYTES;
        }
        tx1.setString(blk0, 30, "abc", false);
        tx2.setString(blk1, 30, "def", false);
        tx1.commit();
        tx2.commit();
        printValues("After Initialization:");
    }

    private static void modify() {
        Transaction tx3 = db.newTx();
        Transaction tx4 = db.newTx();
        tx3.pin(blk0);
        tx4.pin(blk1);
        int pos = 0;
        for (int i = 0; i < 6; i++) {
            tx3.setInt(blk0, pos, pos + 100, true);
            tx4.setInt(blk1, pos, pos + 100, true);
            pos += Integer.BYTES;
        }
        tx3.setString(blk0, 30, "uvw", true);
        tx4.setString(blk1, 30, "xyz", true);
        bm.flushAll(3);
        bm.flushAll(4);
        printValues("After modification:");

        tx3.rollback();
        printValues("After rollback:");
        // tx4 stops here without committing or rolling back,
        // so all its changes should be undone during recovery.
    }

    private static void recover() {
        Transaction tx = db.newTx();
        tx.recover();
        printValues("After recovery:");
    }

    // Print the values that made it to disk.
    private static void printValues(String msg) {
        System.out.println(msg);
        Page p0 = new Page(fm.blockSize());
        Page p1 = new Page(fm.blockSize());
        fm.read(blk0, p0);
        fm.read(blk1, p1);
        int pos = 0;
        for (int i = 0; i < 6; i++) {
            System.out.print(p0.getInt(pos) + " ");
            System.out.print(p1.getInt(pos) + " ");
            pos += Integer.BYTES;
        }
        System.out.print(p0.getString(30) + " ");
        System.out.print(p1.getString(30) + " ");
        System.out.println();
    }
}
