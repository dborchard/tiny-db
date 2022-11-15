package simpledb.e_record;


import simpledb.a_server.SimpleDB;
import simpledb.f_tx.Transaction;
import simpledb.g_file.BlockId;

public class RecordPageTest {
   public static void main(String[] args) throws Exception {
      SimpleDB db = new SimpleDB("recordtest");
      Transaction tx = db.newTx();

      Schema sch = new Schema();
      sch.addIntField("A");
      sch.addStringField("B", 9);
      Layout layout = new Layout(sch);
      for (String fldname : layout.schema().fields()) {
         int offset = layout.offset(fldname);
         System.out.println(fldname + " has offset " + offset);
      }
      BlockId blk = tx.append("testfile");
      tx.pin(blk);
      RecordPage rp = new RecordPage(tx, blk, layout);
      rp.format();

      System.out.println("Filling the page with random records.");
      int slot = rp.insertAfter(-1);
      while (slot >= 0) {  
         int n = (int) Math.round(Math.random() * 50);
         rp.setInt(slot, "A", n);
         rp.setString(slot, "B", "rec"+n);
         System.out.println("inserting into slot " + slot + ": {" + n + ", " + "rec"+n + "}");
         slot = rp.insertAfter(slot);
      }

      System.out.println("Deleting these records, whose A-values are less than 25.");
      int count = 0;
      slot = rp.nextAfter(-1);
      while (slot >= 0) {
         int a = rp.getInt(slot, "A");
         String b = rp.getString(slot, "B");
         if (a < 25) {
            count++;
            System.out.println("slot " + slot + ": {" + a + ", " + b + "}");
            rp.delete(slot);
         }
         slot = rp.nextAfter(slot);
      }
      System.out.println(count + " values under 25 were deleted.\n");

      System.out.println("Here are the remaining records.");
      slot = rp.nextAfter(-1);
      while (slot >= 0) {
         int a = rp.getInt(slot, "A");
         String b = rp.getString(slot, "B");
         System.out.println("slot " + slot + ": {" + a + ", " + b + "}");
         slot = rp.nextAfter(slot);
      }
      tx.unpin(blk);
      tx.commit();
   }
}
