package simpledb.b2_index.btree;

import simpledb.d_scans.domains.Constant;
import simpledb.g_file.BlockId;
import simpledb.e_record.Layout;
import simpledb.e_record.RID;
import simpledb.e_record.Schema;
import simpledb.f_tx.Transaction;

import static java.sql.Types.INTEGER;

/**
 * B-tree directory and leaf pages have many commonalities:
 * in particular, their records are stored in sorted order, 
 * and pages split when full.
 * A BTNode object contains this common functionality.
 * @author Edward Sciore
 */
public class BTPage {
   private Transaction tx;
   private BlockId currentblk;
   private Layout layout;
   
   /**
    * Open a node for the specified B-tree block.
    * @param currentblk a reference to the B-tree block
    * @param layout the metadata for the particular B-tree file
    * @param tx the calling transaction
    */
   public BTPage(Transaction tx, BlockId currentblk, Layout layout) {
      this.tx = tx;
      this.currentblk = currentblk;
      this.layout = layout;
      tx.pin(currentblk);
   }
   
   /**
    * Calculate the position where the first record having
    * the specified search key should be, then returns
    * the position before it.
    * @param searchkey the search key
    * @return the position before where the search key goes
    */
   public int findSlotBefore(Constant searchkey) {
      int slot = 0;
      while (slot < getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
         slot++;
      return slot-1;
   }
   
   /**
    * Close the page by unpinning its buffer.
    */
   public void close() {
      if (currentblk != null)
         tx.unpin(currentblk);
      currentblk = null;
   }
   
   /**
    * Return true if the block is full.
    * @return true if the block is full
    */
   public boolean isFull() {
      return slotpos(getNumRecs()+1) >= tx.blockSize();
   }
   
   /**
    * Split the page at the specified position.
    * A new page is created, and the records of the page
    * starting at the split position are transferred to the new page.
    * @param splitpos the split position
    * @param flag the initial value of the flag field
    * @return the reference to the new block
    */
   public BlockId split(int splitpos, int flag) {
      BlockId newblk = appendNew(flag);
      BTPage newpage = new BTPage(tx, newblk, layout);
      transferRecs(splitpos, newpage);
      newpage.setFlag(flag);
      newpage.close();
      return newblk;
   }
   
   /**
    * Return the dataval of the record at the specified slot.
    * @param slot the integer slot of an index record
    * @return the dataval of the record at that slot
    */
   public Constant getDataVal(int slot) {
      return getVal(slot, "dataval");
   }
   
   /**
    * Return the value of the page's flag field
    * @return the value of the page's flag field
    */
   public int getFlag() {
      return tx.getInt(currentblk, 0);
   }
   
   /**
    * Set the page's flag field to the specified value
    * @param val the new value of the page flag
    */
   public void setFlag(int val) {
      tx.setInt(currentblk, 0, val);
   }
   
   /**
    * Append a new block to the end of the specified B-tree file,
    * having the specified flag value.
    * @param flag the initial value of the flag
    * @return a reference to the newly-created block
    */
   public BlockId appendNew(int flag) {
      BlockId blk = tx.append(currentblk.fileName());
      tx.pin(blk);
      format(blk, flag);
      return blk;
   }
 
   public void format(BlockId blk, int flag) {
      tx.setInt(blk, 0, flag);
      tx.setInt(blk, Integer.BYTES, 0);  // #records = 0
      int recsize = layout.slotSize();
      for (int pos=2*Integer.BYTES; pos+recsize<=tx.blockSize(); pos += recsize)
         makeDefaultRecord(blk, pos);
   }
   
   private void makeDefaultRecord(BlockId blk, int pos) {
      for (String fldname : layout.schema().fields()) {
         int offset = layout.offset(fldname);
         if (layout.schema().type(fldname) == INTEGER)
            tx.setInt(blk, pos + offset, 0);
         else
            tx.setString(blk, pos + offset, "");
      }
   }
   // Methods called only by BTreeDir
   
   /**
    * Return the block number stored in the index record 
    * at the specified slot.
    * @param slot the slot of an index record
    * @return the block number stored in that record
    */
   public int getChildNum(int slot) {
      return getInt(slot, "block");
   }
   
   /**
    * Insert a directory entry at the specified slot.
    * @param slot the slot of an index record
    * @param val the dataval to be stored
    * @param blknum the block number to be stored
    */
   public void insertDir(int slot, Constant val, int blknum) {
      insert(slot);
      setVal(slot, "dataval", val);
      setInt(slot, "block", blknum);
   }
   
   // Methods called only by BTreeLeaf
   
   /**
    * Return the dataRID value stored in the specified leaf index record.
    * @param slot the slot of the desired index record
    * @return the dataRID value store at that slot
    */
   public RID getDataRid(int slot) {
      return new RID(getInt(slot, "block"), getInt(slot, "id"));
   }
   
   /**
    * Insert a leaf index record at the specified slot.
    * @param slot the slot of the desired index record
    * @param val the new dataval
    * @param rid the new dataRID
    */
   public void insertLeaf(int slot, Constant val, RID rid) {
      insert(slot);
      setVal(slot, "dataval", val);
      setInt(slot, "block", rid.blockNumber());
      setInt(slot, "id", rid.slot());
   }
   
   /**
    * Delete the index record at the specified slot.
    * @param slot the slot of the deleted index record
    */
   public void delete(int slot) {
      for (int i=slot+1; i<getNumRecs(); i++)
         copyRecord(i, i-1);
      setNumRecs(getNumRecs()-1);
      return;
   }
   
   /**
    * Return the number of index records in this page.
    * @return the number of index records in this page
    */
   public int getNumRecs() {
      return tx.getInt(currentblk, Integer.BYTES);
   }
   
   // Private methods
   
   private int getInt(int slot, String fldname) {
      int pos = fldpos(slot, fldname);
      return tx.getInt(currentblk, pos);
   }
   
   private String getString(int slot, String fldname) {
      int pos = fldpos(slot, fldname);
      return tx.getString(currentblk, pos);
   }
   
   private Constant getVal(int slot, String fldname) {
      int type = layout.schema().type(fldname);
      if (type == INTEGER)
         return new Constant(getInt(slot, fldname));
      else
         return new Constant(getString(slot, fldname));
   }
   
   private void setInt(int slot, String fldname, int val) {
      int pos = fldpos(slot, fldname);
      tx.setInt(currentblk, pos, val);
   }
   
   private void setString(int slot, String fldname, String val) {
      int pos = fldpos(slot, fldname);
      tx.setString(currentblk, pos, val);
   }
   
   private void setVal(int slot, String fldname, Constant val) {
      int type = layout.schema().type(fldname);
      if (type == INTEGER)
         setInt(slot, fldname, val.asInt());
      else
         setString(slot, fldname, val.asString());
   }
   
   private void setNumRecs(int n) {
      tx.setInt(currentblk, Integer.BYTES, n);
   }
   
   private void insert(int slot) {
      for (int i=getNumRecs(); i>slot; i--)
         copyRecord(i-1, i);
      setNumRecs(getNumRecs()+1);
   }
   
   private void copyRecord(int from, int to) {
      Schema sch = layout.schema();
      for (String fldname : sch.fields())
         setVal(to, fldname, getVal(from, fldname));
   }
   
   private void transferRecs(int slot, BTPage dest) {
      int destslot = 0;
      while (slot < getNumRecs()) {
         dest.insert(destslot);
         Schema sch = layout.schema();
         for (String fldname : sch.fields())
            dest.setVal(destslot, fldname, getVal(slot, fldname));
         delete(slot);
         destslot++;
      }
   }
   
   private int fldpos(int slot, String fldname) {
      int offset = layout.offset(fldname);
      return slotpos(slot) + offset;
   }
   
   private int slotpos(int slot) {
      int slotsize = layout.slotSize();
      return Integer.BYTES + Integer.BYTES + (slot * slotsize);
   }
}
