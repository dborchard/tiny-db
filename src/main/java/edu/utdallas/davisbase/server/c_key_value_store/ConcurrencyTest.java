package edu.utdallas.davisbase.server.c_key_value_store;

import edu.utdallas.davisbase.server.b_query_engine.SimpleDB;
import edu.utdallas.davisbase.server.d_storage_engine.LogMgr;
import edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr.BufferMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.FileMgr;

public class ConcurrencyTest {
    private static FileMgr fm;
    private static LogMgr lm;
    private static BufferMgr bm;

    public static void main(String[] args) {
        //initialize the database system
        SimpleDB db = new SimpleDB("concurrencytest", 400, 8);
        fm = db.fileMgr();
        lm = db.logMgr();
        bm = db.bufferMgr();
        A a = new A();
        new Thread(a).start();
        B b = new B();
        new Thread(b).start();
        C c = new C();
        new Thread(c).start();
    }

    static class A implements Runnable {
        public void run() {
            try {
                Transaction txA = new Transaction(fm, lm, bm);
                BlockId blk1 = new BlockId("testfile", 1);
                BlockId blk2 = new BlockId("testfile", 2);
                txA.pin(blk1);
                txA.pin(blk2);
                System.out.println("Tx A: request slock 1");
                txA.getInt(blk1, 0);
                System.out.println("Tx A: receive slock 1");
                Thread.sleep(1000);
                System.out.println("Tx A: request slock 2");
                txA.getInt(blk2, 0);
                System.out.println("Tx A: receive slock 2");
                txA.commit();
                System.out.println("Tx A: commit");

            } catch (InterruptedException e) {
            }
            ;
        }
    }

    static class B implements Runnable {
        public void run() {
            try {
                Transaction txB = new Transaction(fm, lm, bm);
                BlockId blk1 = new BlockId("testfile", 1);
                BlockId blk2 = new BlockId("testfile", 2);
                txB.pin(blk1);
                txB.pin(blk2);
                System.out.println("Tx B: request xlock 2");
                txB.setInt(blk2, 0, 0, false);
                System.out.println("Tx B: receive xlock 2");
                Thread.sleep(1000);
                System.out.println("Tx B: request slock 1");
                txB.getInt(blk1, 0);
                System.out.println("Tx B: receive slock 1");
                txB.commit();
                System.out.println("Tx B: commit");
            } catch (InterruptedException e) {
            }
            ;
        }
    }

    static class C implements Runnable {
        public void run() {
            try {
                Transaction txC = new Transaction(fm, lm, bm);
                BlockId blk1 = new BlockId("testfile", 1);
                BlockId blk2 = new BlockId("testfile", 2);
                txC.pin(blk1);
                txC.pin(blk2);
                Thread.sleep(500);
                System.out.println("Tx C: request xlock 1");
                txC.setInt(blk1, 0, 0, false);
                System.out.println("Tx C: receive xlock 1");
                Thread.sleep(1000);
                System.out.println("Tx C: request slock 2");
                txC.getInt(blk2, 0);
                System.out.println("Tx C: receive slock 2");
                txC.commit();
                System.out.println("Tx C: commit");
            } catch (InterruptedException e) {
            }
            ;
        }
    }
}
