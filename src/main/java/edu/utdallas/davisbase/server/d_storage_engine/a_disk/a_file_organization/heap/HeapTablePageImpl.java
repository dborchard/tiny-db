package edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap;

import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.TablePage;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;

import static java.sql.Types.INTEGER;

/**
 * Store a record at a given location in a block.
 *
 * @author Edward Sciore
 */
public class HeapTablePageImpl implements TablePage {
    public static final int EMPTY = 0, USED = 1;
    private Transaction tx;
    private BlockId blk;
    private TableFileLayout layout;

    public HeapTablePageImpl(Transaction tx, BlockId blk, TableFileLayout layout) {
        this.tx = tx;
        this.blk = blk;
        this.layout = layout;
    }

    /**
     * Return the integer value stored for the
     * specified field of a specified slot.
     *
     * @param fldname the name of the field.
     * @return the integer stored in that field
     */
    public int getInt(int slot, String fldname) {
        int fldpos = offset(slot) + layout.offset(fldname);
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
        int fldpos = offset(slot) + layout.offset(fldname);
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
        int fldpos = offset(slot) + layout.offset(fldname);
        tx.setInt(blk, fldpos, val, true);
    }

    /**
     * Store a string at the specified field
     * of the specified slot.
     *
     * @param fldname the name of the field
     * @param val     the string value stored in that field
     */
    public void setString(int slot, String fldname, String val) {
        int fldpos = offset(slot) + layout.offset(fldname);
        tx.setString(blk, fldpos, val, true);
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
            tx.setInt(blk, offset(slot), EMPTY, false);
            TableSchema sch = layout.schema();
            for (String fldname : sch.fields()) {
                int fldpos = offset(slot) + layout.offset(fldname);
                if (sch.type(fldname) == INTEGER) tx.setInt(blk, fldpos, 0, false);
                else tx.setString(blk, fldpos, "", false);
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
        tx.setInt(blk, offset(slot), flag, true);
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
        return slot * layout.slotSize();
    }
}








