package edu.utdallas.davisbase.db.storage_engine.c_file;

public class BlockId {
   private String filename;
   private int blknum;

   public BlockId(String filename, int blknum) {
      this.filename = filename;
      this.blknum   = blknum;
   }

   public String fileName() {
      return filename;
   }

   public int number() {
      return blknum;
   }
   
   public boolean equals(Object obj) {
      BlockId blk = (BlockId) obj;
      return filename.equals(blk.filename) && blknum == blk.blknum;
   }
   
   public String toString() {
      return "[file " + filename + ", block " + blknum + "]";
   }
   
   public int hashCode() {
      return toString().hashCode();
   }
}
