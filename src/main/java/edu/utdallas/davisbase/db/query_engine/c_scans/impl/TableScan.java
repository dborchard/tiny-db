package edu.utdallas.davisbase.db.query_engine.c_scans.impl;

import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;
import edu.utdallas.davisbase.db.query_engine.c_scans.UpdateScan;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.RecordId;
import edu.utdallas.davisbase.db.storage_engine.DataRecordPage;
import edu.utdallas.davisbase.db.storage_engine.b_transaction.Transaction;
import edu.utdallas.davisbase.db.storage_engine.d_file.BlockId;

import static java.sql.Types.INTEGER;

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 *
 * @author sciore
 */
public class TableScan implements UpdateScan {
    private Transaction tx;
    private TableFileLayout tableFileLayout;
    private DataRecordPage rp;
    private String filename;
    private int currentSlot;

    public TableScan(Transaction tx, String tblname, TableFileLayout tableFileLayout) {
        this.tx = tx;
        this.tableFileLayout = tableFileLayout;
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
            moveToBlock(rp.block().number() + 1);
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
        if (tableFileLayout.schema().type(fldname) == INTEGER) return new D_Constant(getInt(fldname));
        else return new D_Constant(getString(fldname));
    }

    public boolean hasField(String fldname) {
        return tableFileLayout.schema().hasField(fldname);
    }

    public void close() {
        if (rp != null) tx.unpin(rp.block());
    }

    // Methods that implement UpdateScan

    public void setInt(String fldname, int val) {
        rp.setInt(currentSlot, fldname, val);
    }

    public void setString(String fldname, String val) {
        rp.setString(currentSlot, fldname, val);
    }

    public void setVal(String fldname, D_Constant val) {
        if (tableFileLayout.schema().type(fldname) == INTEGER) setInt(fldname, val.asInt());
        else setString(fldname, val.asString());
    }

    public void seekToHead_Update() {
        currentSlot = rp.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) moveToNewBlock();
            else moveToBlock(rp.block().number() + 1);
            currentSlot = rp.insertAfter(currentSlot);
        }
    }

    public void delete() {
        rp.delete(currentSlot);
    }

    public void moveToRid(RecordId recordID) {
        close();
        BlockId blk = new BlockId(filename, recordID.blockNumber());
        rp = new DataRecordPage(tx, blk, tableFileLayout);
        currentSlot = recordID.slot();
    }

    public RecordId getRid() {
        return new RecordId(rp.block().number(), currentSlot);
    }

    // Private auxiliary methods

    private void moveToBlock(int blknum) {
        close();
        BlockId blk = new BlockId(filename, blknum);
        rp = new DataRecordPage(tx, blk, tableFileLayout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(filename);
        rp = new DataRecordPage(tx, blk, tableFileLayout);
        rp.format();
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return rp.block().number() == tx.size(filename) - 1;
    }
}
