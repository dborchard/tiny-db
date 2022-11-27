package edu.utdallas.davisbase.db.storage_engine.a_io.index.btree.common;

import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.RecordId;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableSchema;
import edu.utdallas.davisbase.db.storage_engine.b_transaction.Transaction;
import edu.utdallas.davisbase.db.storage_engine.d_file.BlockId;
import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;

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
   private TableFileLayout tableFileLayout;
   
   /**
    * Open a node for the specified B-tree block.
    * @param currentblk a reference to the B-tree block
    * @param tableFileLayout the metadata for the particular B-tree file
    * @param tx the calling transaction
    */
   public BTPage(Transaction tx, BlockId currentblk, TableFileLayout tableFileLayout) {
      this.tx = tx;
      this.currentblk = currentblk;
      this.tableFileLayout = tableFileLayout;
      tx.pin(currentblk);
   }
   
   /**
    * Calculate the position where the first record having
    * the specified search key should be, then returns
    * the position before it.
    * @param searchkey the search key
    * @return the position before where the search key goes
    */
   public int findSlotBefore(D_Constant searchkey) {
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
      BTPage newpage = new BTPage(tx, newblk, tableFileLayout);
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
   public D_Constant getDataVal(int slot) {
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
      int recsize = tableFileLayout.slotSize();
      for (int pos=2*Integer.BYTES; pos+recsize<=tx.blockSize(); pos += recsize)
         makeDefaultRecord(blk, pos);
   }
   
   private void makeDefaultRecord(BlockId blk, int pos) {
      for (String fldname : tableFileLayout.schema().fields()) {
         int offset = tableFileLayout.offset(fldname);
         if (tableFileLayout.schema().type(fldname) == INTEGER)
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
   public void insertDir(int slot, D_Constant val, int blknum) {
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
   public RecordId getDataRid(int slot) {
      return new RecordId(getInt(slot, "block"), getInt(slot, "id"));
   }
   
   /**
    * Insert a leaf index record at the specified slot.
    * @param slot the slot of the desired index record
    * @param val the new dataval
    * @param recordID the new dataRID
    */
   public void insertLeaf(int slot, D_Constant val, RecordId recordID) {
      insert(slot);
      setVal(slot, "dataval", val);
      setInt(slot, "block", recordID.blockNumber());
      setInt(slot, "id", recordID.slot());
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
   
   private D_Constant getVal(int slot, String fldname) {
      int type = tableFileLayout.schema().type(fldname);
      if (type == INTEGER)
         return new D_Constant(getInt(slot, fldname));
      else
         return new D_Constant(getString(slot, fldname));
   }
   
   private void setInt(int slot, String fldname, int val) {
      int pos = fldpos(slot, fldname);
      tx.setInt(currentblk, pos, val);
   }
   
   private void setString(int slot, String fldname, String val) {
      int pos = fldpos(slot, fldname);
      tx.setString(currentblk, pos, val);
   }
   
   private void setVal(int slot, String fldname, D_Constant val) {
      int type = tableFileLayout.schema().type(fldname);
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
      TableSchema sch = tableFileLayout.schema();
      for (String fldname : sch.fields())
         setVal(to, fldname, getVal(from, fldname));
   }
   
   private void transferRecs(int slot, BTPage dest) {
      int destslot = 0;
      while (slot < getNumRecs()) {
         dest.insert(destslot);
         TableSchema sch = tableFileLayout.schema();
         for (String fldname : sch.fields())
            dest.setVal(destslot, fldname, getVal(slot, fldname));
         delete(slot);
         destslot++;
      }
   }
   
   private int fldpos(int slot, String fldname) {
      int offset = tableFileLayout.offset(fldname);
      return slotpos(slot) + offset;
   }
   
   private int slotpos(int slot) {
      int slotsize = tableFileLayout.slotSize();
      return Integer.BYTES + Integer.BYTES + (slot * slotsize);
   }
}
