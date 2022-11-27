package edu.utdallas.davisbase.server.d_storage_engine.a_disk.c_wal;


import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.FileMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.Page;

import java.util.Iterator;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 * 
 * @author Edward Sciore
 */
public class LogIterator implements Iterator<byte[]> {
   private FileMgr fm;
   private BlockId blk;
   private Page p;
   private int currentpos;
   private int boundary;

   /**
    * Creates an iterator for the records in the log file,
    * positioned after the last log record.
    */
   public LogIterator(FileMgr fm, BlockId blk) {
      this.fm = fm;
      this.blk = blk;
      byte[] b = new byte[fm.blockSize()];
      p = new Page(b);
      moveToBlock(blk);
   }

   /**
    * Determines if the current log record
    * is the earliest record in the log file.
    * @return true if there is an earlier record
    */
   public boolean hasNext() {
      return currentpos<fm.blockSize() || blk.number()>0;
   }

   /**
    * Moves to the next log record in the block.
    * If there are no more log records in the block,
    * then move to the previous block
    * and return the log record from there.
    * @return the next earliest log record
    */
   public byte[] next() {
      if (currentpos == fm.blockSize()) {
         blk = new BlockId(blk.fileName(), blk.number()-1);
         moveToBlock(blk);
      }
      byte[] rec = p.getBytes(currentpos);
      currentpos += Integer.BYTES + rec.length;
      return rec;
   }

   /**
    * Moves to the specified log block
    * and positions it at the first record in that block
    * (i.e., the most recent one).
    */
   private void moveToBlock(BlockId blk) {
      fm.read(blk, p);
      boundary = p.getInt(0);
      currentpos = boundary;
   }
}
