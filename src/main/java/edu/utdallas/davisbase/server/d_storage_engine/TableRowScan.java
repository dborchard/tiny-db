package edu.utdallas.davisbase.server.d_storage_engine;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.HeapRecordPage;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordKey;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.UpdateScan;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.b_file.BlockId;

import static java.sql.Types.INTEGER;

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 *
 * @author sciore
 */
public class TableRowScan implements UpdateScan {
    private Transaction tx;
    private RecordValueLayout layout;
    private HeapRecordPage rp;
    private String filename;
    private int currentslot;

    public TableRowScan(Transaction tx, String tblname, RecordValueLayout layout) {
        this.tx = tx;
        this.layout = layout;
        filename = tblname + ".tbl";
        if (tx.size(filename) == 0) moveToNewBlock();
        else moveToBlock(0);
    }

    // Methods that implement Scan

    public void seekToHead_Query() {
        moveToBlock(0);
    }


    public boolean next() {
        currentslot = rp.nextAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock()) return false;
            moveToBlock(rp.getBlockId().number() + 1);
            currentslot = rp.nextAfter(currentslot);
        }
        return true;
    }

    public int getInt(String fldname) {
        return rp.getInt(currentslot, fldname);
    }

    public String getString(String fldname) {
        return rp.getString(currentslot, fldname);
    }

    public D_Constant getVal(String fldname) {
        if (layout.schema().type(fldname) == INTEGER) return new D_Constant(getInt(fldname));
        else return new D_Constant(getString(fldname));
    }

    public boolean hasField(String fldname) {
        return layout.schema().hasField(fldname);
    }

    public void close() {
        if (rp != null) tx.unpin(rp.getBlockId());
    }

    // Methods that implement UpdateScan

    public void setInt(String fldname, int val) {
        rp.setInt(currentslot, fldname, val);
    }

    public void setString(String fldname, String val) {
        rp.setString(currentslot, fldname, val);
    }


    public void setVal(String fldname, D_Constant val) {
        if (layout.schema().type(fldname) == INTEGER) setInt(fldname, val.asInt());
        else setString(fldname, val.asString());
    }

    public void seekToHead_Insert() {
        currentslot = rp.insertAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock()) moveToNewBlock();
            else moveToBlock(rp.getBlockId().number() + 1);
            currentslot = rp.insertAfter(currentslot);
        }
    }

    public void delete() {
        rp.delete(currentslot);
    }

    public void moveToRid(RecordKey key) {
        close();
        BlockId blk = new BlockId(filename, key.blockNumber());
        rp = new HeapRecordPage(tx, blk, layout);
        currentslot = key.slot();
    }

    public RecordKey getRid() {
        return new RecordKey(rp.getBlockId().number(), currentslot);
    }

    // Private auxiliary methods

    private void moveToBlock(int blknum) {
        close();
        BlockId blk = new BlockId(filename, blknum);
        rp = new HeapRecordPage(tx, blk, layout);
        currentslot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(filename);
        rp = new HeapRecordPage(tx, blk, layout);
        rp.format();
        currentslot = -1;
    }

    private boolean atLastBlock() {
        return rp.getBlockId().number() == tx.size(filename) - 1;
    }
}
//DONE
