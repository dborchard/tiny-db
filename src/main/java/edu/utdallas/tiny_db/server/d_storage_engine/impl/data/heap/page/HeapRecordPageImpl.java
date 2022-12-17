package edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.page;

import static java.sql.Types.INTEGER;

import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TablePhysicalLayout;
import edu.utdallas.tiny_db.server.d_storage_engine.common.file.BlockId;
import edu.utdallas.tiny_db.server.d_storage_engine.common.transaction.Transaction;

/**
 * Page (Block) containing multiple records.
 * <p>
 * Here we know the entries of a page as records, due to the knowledge of {@link TablePhysicalLayout}
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class HeapRecordPageImpl {

  public static final int EMPTY = 0, USED = 1;
  private Transaction tx;
  private BlockId blockId;
  private TablePhysicalLayout recordValueLayout;

  public HeapRecordPageImpl(Transaction tx, BlockId blockId, TablePhysicalLayout recordValueLayout) {
    this.tx = tx;
    this.blockId = blockId;
    this.recordValueLayout = recordValueLayout;
  }

  /**
   * Return the integer value stored for the specified field of a specified slot.
   *
   * @param fldname the name of the field.
   * @return the integer stored in that field
   */
  public int getInt(int slot, String fldname) {
    int fldpos = offset(slot) + recordValueLayout.offset(fldname);
    return tx.getInt(blockId, fldpos);
  }

  /**
   * Return the string value stored for the specified field of the specified slot.
   *
   * @param fldname the name of the field.
   * @return the string stored in that field
   */
  public String getString(int slot, String fldname) {
    int fldpos = offset(slot) + recordValueLayout.offset(fldname);
    return tx.getString(blockId, fldpos);
  }

  /**
   * Store an integer at the specified field of the specified slot.
   *
   * @param fldname the name of the field
   * @param val     the integer value stored in that field
   */
  public void setInt(int slot, String fldname, int val) {
    int fldpos = offset(slot) + recordValueLayout.offset(fldname);
    tx.setInt(blockId, fldpos, val);
  }

  /**
   * Store a string at the specified field of the specified slot.
   *
   * @param fldname the name of the field
   * @param val     the string value stored in that field
   */
  public void setString(int slot, String fldname, String val) {
    int fldpos = offset(slot) + recordValueLayout.offset(fldname);
    tx.setString(blockId, fldpos, val);
  }

  public void delete(int slot) {
    setFlag(slot, EMPTY);
  }

  /**
   * Use the layout to format a new block of records. These values should not be logged (because the
   * old values are meaningless).
   */
  public void format() {
    int slot = 0;
    while (isValidSlot(slot)) {
      tx.setInt(blockId, offset(slot), EMPTY);
      TableDefinition sch = recordValueLayout.schema();
      for (String fldname : sch.fields()) {
        int fldpos = offset(slot) + recordValueLayout.offset(fldname);
        if (sch.type(fldname) == INTEGER) {
          tx.setInt(blockId, fldpos, 0);
        } else {
          tx.setString(blockId, fldpos, "");
        }
      }
      slot++;
    }
  }

  public int findSlotAfter(int slot) {
    return searchAfter(slot, USED);
  }

  public int insertAfter(int slot) {
    int newslot = searchAfter(slot, EMPTY);
    if (newslot >= 0) {
      setFlag(newslot, USED);
    }
    return newslot;
  }

  public BlockId getBlockId() {
    return blockId;
  }

  // Private auxiliary methods

  /**
   * Set the record's empty/inuse flag.
   */
  private void setFlag(int slot, int flag) {
    tx.setInt(blockId, offset(slot), flag);
  }

  private int searchAfter(int slot, int flag) {
    slot++;
    while (isValidSlot(slot)) {
      if (tx.getInt(blockId, offset(slot)) == flag) {
        return slot;
      }
      slot++;
    }
    return -1;
  }

  private boolean isValidSlot(int slot) {
    return offset(slot + 1) <= tx.blockSize();
  }

  private int offset(int slot) {
    return slot * recordValueLayout.slotSize();
  }
}








