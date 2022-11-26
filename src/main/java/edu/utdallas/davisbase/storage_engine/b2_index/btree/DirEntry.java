package edu.utdallas.davisbase.storage_engine.b2_index.btree;

import edu.utdallas.davisbase.query_engine.c_parse.domain.clause.D_Constant;

/**
 * A directory entry has two components: the number of the child block,
 * and the dataval of the first record in that block.
 * @author Edward Sciore
 */
public class DirEntry {
   private D_Constant dataval;
   private int blocknum;
   
   /**
    * Creates a new entry for the specified dataval and block number.
    * @param dataval the dataval
    * @param blocknum the block number
    */
   public DirEntry(D_Constant dataval, int blocknum) {
      this.dataval  = dataval;
      this.blocknum = blocknum;
   }
   
   /**
    * Returns the dataval component of the entry
    * @return the dataval component of the entry
    */
   public D_Constant dataVal() {
      return dataval;
   }
   
   /**
    * Returns the block number component of the entry
    * @return the block number component of the entry
    */
   public int blockNumber() {
      return blocknum;
   }
}

