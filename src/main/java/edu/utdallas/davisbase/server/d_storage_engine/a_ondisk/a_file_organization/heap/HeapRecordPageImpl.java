package edu.utdallas.davisbase.server.d_storage_engine.a_ondisk.a_file_organization.heap;

import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.a_ondisk.a_file_organization.IRecordPage;

import static java.sql.Types.INTEGER;

/**
 * Store a record at a given location in a block.
 *
 * @author Edward Sciore
 */
public class HeapRecordPageImpl implements IRecordPage {
    public static final int EMPTY = 0, USED = 1;
    private Transaction tx;
    private BlockId blk;
    private RecordValueLayout recordValueLayout;

    public HeapRecordPageImpl(Transaction tx, BlockId blk, RecordValueLayout recordValueLayout) {
        this.tx = tx;
        this.blk = blk;
        this.recordValueLayout = recordValueLayout;
    }

    /**
     * Return the integer value stored for the
     * specified field of a specified slot.
     *
     * @param fldname the name of the field.
     * @return the integer stored in that field
     */
    public int getInt(int slot, String fldname) {
        int fldpos = offset(slot) + recordValueLayout.offset(fldname);
        return tx.getInt(blk, fldpos);
    }

    /**
     * Return the string value stored for the
     * specified field of the specified slot.
     *
     * @param fldname the name of the field.
     * @return the string stored in that field
     */
    public String getString(int slot, String fldname) {
        int fldpos = offset(slot) + recordValueLayout.offset(fldname);
        return tx.getString(blk, fldpos);
    }

    /**
     * Store an integer at the specified field
     * of the specified slot.
     *
     * @param fldname the name of the field
     * @param val     the integer value stored in that field
     */
    public void setInt(int slot, String fldname, int val) {
        int fldpos = offset(slot) + recordValueLayout.offset(fldname);
        tx.setInt(blk, fldpos, val);
    }

    /**
     * Store a string at the specified field
     * of the specified slot.
     *
     * @param fldname the name of the field
     * @param val     the string value stored in that field
     */
    public void setString(int slot, String fldname, String val) {
        int fldpos = offset(slot) + recordValueLayout.offset(fldname);
        tx.setString(blk, fldpos, val);
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    /**
     * Use the layout to format a new block of records.
     * These values should not be logged
     * (because the old values are meaningless).
     */
    public void format() {
        int slot = 0;
        while (isValidSlot(slot)) {
            tx.setInt(blk, offset(slot), EMPTY);
            RecordValueSchema sch = recordValueLayout.schema();
            for (String fldname : sch.fields()) {
                int fldpos = offset(slot) + recordValueLayout.offset(fldname);
                if (sch.type(fldname) == INTEGER) tx.setInt(blk, fldpos, 0);
                else tx.setString(blk, fldpos, "");
            }
            slot++;
        }
    }

    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    public int insertAfter(int slot) {
        int newslot = searchAfter(slot, EMPTY);
        if (newslot >= 0) setFlag(newslot, USED);
        return newslot;
    }

    public BlockId getBlockId() {
        return blk;
    }

    // Private auxiliary methods

    /**
     * Set the record's empty/inuse flag.
     */
    private void setFlag(int slot, int flag) {
        tx.setInt(blk, offset(slot), flag);
    }

    private int searchAfter(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (tx.getInt(blk, offset(slot)) == flag) return slot;
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








