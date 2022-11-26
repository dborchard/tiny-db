package edu.utdallas.davisbase.storage_engine.f_tx;


import edu.utdallas.davisbase.query_engine.a_server.SimpleDB;
import edu.utdallas.davisbase.storage_engine.g_file.BlockId;
import edu.utdallas.davisbase.storage_engine.g_file.FileMgr;

public class TransactionTest {
   public static void main(String[] args) throws Exception {
      SimpleDB db = new SimpleDB("txtest");
      FileMgr fm = db.fileMgr();


      Transaction tx1 = new Transaction(fm);
      BlockId blk = new BlockId("testfile", 1);
      tx1.pin(blk);
      tx1.setInt(blk, 80, 1);
      tx1.setString(blk, 40, "one");
      tx1.commit();

      Transaction tx2 = new Transaction(fm);
      tx2.pin(blk);
      int ival = tx2.getInt(blk, 80);
      String sval = tx2.getString(blk, 40);
      System.out.println("initial value at location 80 = " + ival);
      System.out.println("initial value at location 40 = " + sval);
      int newival = ival + 1;
      String newsval = sval + "!";
      tx2.setInt(blk, 80, newival);
      tx2.setString(blk, 40, newsval);
      tx2.commit();
      Transaction tx3 = new Transaction(fm);
      tx3.pin(blk);
      System.out.println("new value at location 80 = " + tx3.getInt(blk, 80));
      System.out.println("new value at location 40 = " + tx3.getString(blk, 40));
      tx3.setInt(blk, 80, 9999);
      System.out.println("pre-rollback value at location 80 = " + tx3.getInt(blk, 80));
      tx3.rollback();

      Transaction tx4 = new Transaction(fm);
      tx4.pin(blk);
      System.out.println("post-rollback at location 80 = " + tx4.getInt(blk, 80));
      tx4.commit();
   }
}
