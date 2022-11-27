package edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr.record.impl;

import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr.record.LogRecord;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.c_wal.LogMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.Page;

public class SetStringRecord implements LogRecord {
    private int txnum, offset;
    private String val;
    private BlockId blk;


    public SetStringRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);
        int fpos = tpos + Integer.BYTES;
        String filename = p.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blknum = p.getInt(bpos);
        blk = new BlockId(filename, blknum);
        int opos = bpos + Integer.BYTES;
        offset = p.getInt(opos);
        int vpos = opos + Integer.BYTES;
        val = p.getString(vpos);
    }

    /**
     * A static method to write a setInt record to the log.
     * This log record contains the SETINT operator,
     * followed by the transaction id, the filename, number,
     * and offset of the modified block, and the previous
     * integer value at that offset.
     *
     * @return the LSN of the last log value
     */
    public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, String val) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blk.fileName().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(val.length());
        byte[] rec = new byte[reclen];
        Page p = new Page(rec);
        p.setInt(0, SETSTRING);
        p.setInt(tpos, txnum);
        p.setString(fpos, blk.fileName());
        p.setInt(bpos, blk.number());
        p.setInt(opos, offset);
        p.setString(vpos, val);
        return lm.append(rec);
    }

    public int op() {
        return SETSTRING;
    }

    public int txNumber() {
        return txnum;
    }

    public String toString() {
        return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + val + ">";
    }

    public void undo(Transaction tx) {
        tx.pin(blk);
        tx.setString(blk, offset, val, false); // don't log the undo!
        tx.unpin(blk);
    }
}