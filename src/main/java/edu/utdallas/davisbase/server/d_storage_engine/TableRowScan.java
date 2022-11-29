package edu.utdallas.davisbase.server.d_storage_engine;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.common.a_scans.UpdateScan;
import edu.utdallas.davisbase.server.d_storage_engine.common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.heap.HeapStorageEngineImpl;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.heap.RecordKey;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.heap.RecordValueLayout;

import static java.sql.Types.INTEGER;

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 *
 * @author sciore
 */
public class TableRowScan implements UpdateScan {
    private Transaction tx;
    private RecordValueLayout recordValueLayout;
    private HeapStorageEngineImpl rp;
    private String filename;
    private int currentSlot;

    public TableRowScan(Transaction tx, String tblname, RecordValueLayout recordValueLayout) {
        this.tx = tx;
        this.recordValueLayout = recordValueLayout;
        filename = tblname + ".tbl";
        if (tx.size(filename) == 0) moveToNewBlock();
        else moveToBlock(0);
    }

    // Methods that implement Scan

    public void seekToHead_Query() {
        moveToBlock(0);
    }

    public boolean next() {
        currentSlot = rp.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) return false;
            moveToBlock(rp.getBlockId().number() + 1);
            currentSlot = rp.nextAfter(currentSlot);
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
        if (recordValueLayout.schema().type(fldname) == INTEGER) return new D_Constant(getInt(fldname));
        else return new D_Constant(getString(fldname));
    }

    public boolean hasField(String fldname) {
        return recordValueLayout.schema().hasField(fldname);
    }

    public void close() {
        if (rp != null) tx.unpin(rp.getBlockId());
    }

    // Methods that implement UpdateScan

    public void setInt(String fldname, int val) {
        rp.setInt(currentSlot, fldname, val);
    }

    public void setString(String fldname, String val) {
        rp.setString(currentSlot, fldname, val);
    }

    public void setVal(String fldname, D_Constant val) {
        if (recordValueLayout.schema().type(fldname) == INTEGER) setInt(fldname, val.asInt());
        else setString(fldname, val.asString());
    }

    public void seekToHead_Insert() {
        currentSlot = rp.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) moveToNewBlock();
            else moveToBlock(rp.getBlockId().number() + 1);
            currentSlot = rp.insertAfter(currentSlot);
        }
    }

    public void delete() {
        rp.delete(currentSlot);
    }

    public void moveToRid(RecordKey recordKey) {
        close();
        BlockId blk = new BlockId(filename, recordKey.blockNumber());
        rp = new HeapStorageEngineImpl(tx, blk, recordValueLayout);
        currentSlot = recordKey.slot();
    }

    public RecordKey getRid() {
        return new RecordKey(rp.getBlockId().number(), currentSlot);
    }

    // Private auxiliary methods

    private void moveToBlock(int blknum) {
        close();
        BlockId blk = new BlockId(filename, blknum);
        rp = new HeapStorageEngineImpl(tx, blk, recordValueLayout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(filename);
        rp = new HeapStorageEngineImpl(tx, blk, recordValueLayout);
        rp.format();
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return rp.getBlockId().number() == tx.size(filename) - 1;
    }
}
