package edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap;

import static java.sql.Types.INTEGER;

import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.tiny_db.server.d_storage_engine.RWRecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.common.file.BlockId;
import edu.utdallas.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.page.HeapRecordPageImpl;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.page.RecordKey;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TablePhysicalLayout;

/**
 * Provides the enumeration (iterator) for records stored in the Disk.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */

public class HeapRWRecordScan implements RWRecordScan {

  private final Transaction tx;
  private final TablePhysicalLayout recordValueLayout;
  private HeapRecordPageImpl rp;
  private final String filename;
  private int currentSlot;

  public HeapRWRecordScan(Transaction tx, String tblname, TablePhysicalLayout recordValueLayout) {
    this.tx = tx;
    this.recordValueLayout = recordValueLayout;
    filename = tblname + ".tbl";
    if (tx.blockCount(filename) == 0) {
      createAndMoveToNewBlock();
    } else {
      moveToBlock(0);
    }
  }

  // Methods that implement Scan

  public void seekToQueryStart() {
    moveToBlock(0);
  }

  public boolean next() {
    currentSlot = rp.findSlotAfter(currentSlot);
    while (currentSlot < 0) {
      if (atLastBlock()) {
        return false;
      }
      moveToBlock(rp.getBlockId().getBlockNumber() + 1);
      currentSlot = rp.findSlotAfter(currentSlot);
    }
    return true;
  }

  public int getInt(String fldname) {
    return rp.getInt(currentSlot, fldname);
  }

  public String getString(String fldname) {
    return rp.getString(currentSlot, fldname);
  }

  public D_Constant getVal(String fldname) {
    if (recordValueLayout.schema().type(fldname) == INTEGER) {
      return new D_Constant(getInt(fldname));
    } else {
      return new D_Constant(getString(fldname));
    }
  }

  public boolean hasField(String fldname) {
    return recordValueLayout.schema().hasField(fldname);
  }

  public void close() {
    if (rp != null) {
      tx.unpin(rp.getBlockId());
    }
  }

  // Methods that implement UpdateScan

  public void setInt(String fldname, int val) {
    rp.setInt(currentSlot, fldname, val);
  }

  public void setString(String fldname, String val) {
    rp.setString(currentSlot, fldname, val);
  }

  public void setVal(String fldname, D_Constant val) {
    if (recordValueLayout.schema().type(fldname) == INTEGER) {
      setInt(fldname, val.asInt());
    } else {
      setString(fldname, val.asString());
    }
  }

  public void seekToInsertStart() {
    currentSlot = rp.insertAfter(currentSlot);
    while (currentSlot < 0) {
      if (atLastBlock()) {
        createAndMoveToNewBlock();
      } else {
        moveToBlock(rp.getBlockId().getBlockNumber() + 1);
      }
      currentSlot = rp.insertAfter(currentSlot);
    }
  }

  public void delete() {
    rp.delete(currentSlot);
  }

  public void seekTo(RecordKey recordKey) {
    close();
    BlockId blk = new BlockId(filename, recordKey.getBlockNumber());
    rp = new HeapRecordPageImpl(tx, blk, recordValueLayout);
    currentSlot = recordKey.getSlotNumber();
  }

  public RecordKey getRid() {
    return new RecordKey(rp.getBlockId().getBlockNumber(), currentSlot);
  }

  // Private auxiliary methods

  private void moveToBlock(int blockNumber) {
    close();
    BlockId blk = new BlockId(filename, blockNumber);
    rp = new HeapRecordPageImpl(tx, blk, recordValueLayout);
    currentSlot = -1;
  }

  private void createAndMoveToNewBlock() {
    close();
    BlockId blk = tx.append(filename);
    rp = new HeapRecordPageImpl(tx, blk, recordValueLayout);
    rp.format();
    currentSlot = -1;
  }

  private boolean atLastBlock() {
    return rp.getBlockId().getBlockNumber() == tx.blockCount(filename) - 1;
  }
}
