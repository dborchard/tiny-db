package edu.utdallas.davisbase.db.storage_engine.a_io.index.btree;

import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.RecordId;
import edu.utdallas.davisbase.db.storage_engine.Transaction;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.btree.common.BTPage;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.btree.common.DirEntry;
import edu.utdallas.davisbase.db.storage_engine.d_file.BlockId;
import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;

/**
 * An object that holds the contents of a B-tree leaf block.
 * @author Edward Sciore
 */
public class BTreeLeaf {
   private Transaction tx;
   private TableFileLayout tableFileLayout;
   private D_Constant searchkey;
   private BTPage contents;
   private int currentslot;
   private String filename;

   /**
    * Opens a buffer to hold the specified leaf block.
    * The buffer is positioned immediately before the first record
    * having the specified search key (if any).
    * @param blk a reference to the disk block
    * @param tableFileLayout the metadata of the B-tree leaf file
    * @param searchkey the search key value
    * @param tx the calling transaction
    */
   public BTreeLeaf(Transaction tx, BlockId blk, TableFileLayout tableFileLayout, D_Constant searchkey) {
      this.tx = tx;
      this.tableFileLayout = tableFileLayout;
      this.searchkey = searchkey;
      contents = new BTPage(tx, blk, tableFileLayout);
      currentslot = contents.findSlotBefore(searchkey);
      filename = blk.fileName();            
   }

   /**
    * Closes the leaf page.
    */
   public void close() {
      contents.close();
   }

   /**
    * Moves to the next leaf record having the 
    * previously-specified search key.
    * Returns false if there is no more such records.
    * @return false if there are no more leaf records for the search key
    */
   public boolean next() {
      currentslot++;
      if (currentslot >= contents.getNumRecs()) 
         return tryOverflow();
      else if (contents.getDataVal(currentslot).equals(searchkey))
         return true;
      else 
         return tryOverflow();
   }

   /**
    * Returns the dataRID value of the current leaf record.
    * @return the dataRID of the current record
    */
   public RecordId getDataRid() {
      return contents.getDataRid(currentslot);
   }

   /**
    * Deletes the leaf record having the specified dataRID
    * @param datarid the dataRId whose record is to be deleted
    */
   public void delete(RecordId datarid) {
      while(next())
         if(getDataRid().equals(datarid)) {
            contents.delete(currentslot);
            return;
         }
   }

   /**
    * Inserts a new leaf record having the specified dataRID
    * and the previously-specified search key.
    * If the record does not fit in the page, then 
    * the page splits and the method returns the
    * directory entry for the new page;
    * otherwise, the method returns null.  
    * If all of the records in the page have the same dataval,
    * then the block does not split; instead, all but one of the
    * records are placed into an overflow block.
    * @param datarid the dataRID value of the new record
    * @return the directory entry of the newly-split page, if one exists.
    */
   public DirEntry insert(RecordId datarid) {
      if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0) {
         D_Constant firstval = contents.getDataVal(0);
         BlockId newblk = contents.split(0, contents.getFlag());
         currentslot = 0;
         contents.setFlag(-1);
         contents.insertLeaf(currentslot, searchkey, datarid); 
         return new DirEntry(firstval, newblk.number());  
      }

      currentslot++;
      contents.insertLeaf(currentslot, searchkey, datarid);
      if (!contents.isFull())
         return null;
      // else page is full, so split it
      D_Constant firstkey = contents.getDataVal(0);
      D_Constant lastkey  = contents.getDataVal(contents.getNumRecs()-1);
      if (lastkey.equals(firstkey)) {
         // create an overflow block to hold all but the first record
         BlockId newblk = contents.split(1, contents.getFlag());
         contents.setFlag(newblk.number());
         return null;
      }
      else {
         int splitpos = contents.getNumRecs() / 2;
         D_Constant splitkey = contents.getDataVal(splitpos);
         if (splitkey.equals(firstkey)) {
            // move right, looking for the next key
            while (contents.getDataVal(splitpos).equals(splitkey))
               splitpos++;
            splitkey = contents.getDataVal(splitpos);
         }
         else {
            // move left, looking for first entry having that key
            while (contents.getDataVal(splitpos-1).equals(splitkey))
               splitpos--;
         }
         BlockId newblk = contents.split(splitpos, -1);
         return new DirEntry(splitkey, newblk.number());
      }
   }

   private boolean tryOverflow() {
      D_Constant firstkey = contents.getDataVal(0);
      int flag = contents.getFlag();
      if (!searchkey.equals(firstkey) || flag < 0)
         return false;
      contents.close();
      BlockId nextblk = new BlockId(filename, flag);
      contents = new BTPage(tx, nextblk, tableFileLayout);
      currentslot = 0;
      return true;
   }
}
