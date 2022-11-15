package simpledb.d_scans.impl;

import simpledb.d_scans.UpdateScan;
import simpledb.d_scans.domains.Constant;
import simpledb.e_record.Layout;
import simpledb.e_record.RID;
import simpledb.e_record.RecordPage;
import simpledb.g_file.BlockId;
import simpledb.f_tx.Transaction;

import static java.sql.Types.INTEGER;

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 *
 * @author sciore
 */
public class TableScan implements UpdateScan {
    private Transaction tx;
    private Layout layout;
    private RecordPage rp;
    private String filename;
    private int currentSlot;

    public TableScan(Transaction tx, String tblname, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        filename = tblname + ".tbl";
        if (tx.size(filename) == 0) moveToNewBlock();
        else moveToBlock(0);
    }

    // Methods that implement Scan

    public void beforeFirst() {
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

    public Constant getVal(String fldname) {
        if (layout.schema().type(fldname) == INTEGER) return new Constant(getInt(fldname));
        else return new Constant(getString(fldname));
    }

    public boolean hasField(String fldname) {
        return layout.schema().hasField(fldname);
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

    public void setVal(String fldname, Constant val) {
        if (layout.schema().type(fldname) == INTEGER) setInt(fldname, val.asInt());
        else setString(fldname, val.asString());
    }

    public void insert() {
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

    public void moveToRid(RID rid) {
        close();
        BlockId blk = new BlockId(filename, rid.blockNumber());
        rp = new RecordPage(tx, blk, layout);
        currentSlot = rid.slot();
    }

    public RID getRid() {
        return new RID(rp.block().number(), currentSlot);
    }

    // Private auxiliary methods

    private void moveToBlock(int blknum) {
        close();
        BlockId blk = new BlockId(filename, blknum);
        rp = new RecordPage(tx, blk, layout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(filename);
        rp = new RecordPage(tx, blk, layout);
        rp.format();
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return rp.block().number() == tx.size(filename) - 1;
    }
}
