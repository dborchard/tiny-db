package edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr;


import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr.log_record.LogRecord;
import edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr.log_record.impl.*;
import edu.utdallas.davisbase.server.d_storage_engine.LogMgr;
import edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr.Buffer;
import edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr.BufferMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr.log_record.LogRecord.*;


/**
 * The recovery manager.  Each transaction has its own recovery manager.
 *
 * @author Edward Sciore
 */
public class RecoveryMgr {
    private LogMgr lm;
    private BufferMgr bm;
    private Transaction tx;
    private int txnum;

    /**
     * Create a recovery manager for the specified transaction.
     *
     * @param txnum the ID of the specified transaction
     */
    public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm) {
        this.tx = tx;
        this.txnum = txnum;
        this.lm = lm;
        this.bm = bm;
        StartRecord.writeToLog(lm, txnum);
    }

    /**
     * Write a commit record to the log, and flushes it to disk.
     */
    public void commit() {
        bm.flushAll(txnum);
        int lsn = CommitRecord.writeToLog(lm, txnum);
        lm.flush(lsn);
    }

    /**
     * Write a rollback record to the log and flush it to disk.
     */
    public void rollback() {
        doRollback();
        bm.flushAll(txnum);
        int lsn = RollbackRecord.writeToLog(lm, txnum);
        lm.flush(lsn);
    }

    /**
     * Recover uncompleted transactions from the log
     * and then write a quiescent checkpoint record to the log and flush it.
     */
    public void recover() {
        doRecover();
        bm.flushAll(txnum);
        int lsn = CheckpointRecord.writeToLog(lm);
        lm.flush(lsn);
    }

    /**
     * Write a setint record to the log and return its lsn.
     *
     * @param buff   the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     */
    public int setInt(Buffer buff, int offset, int newval) {
        int oldval = buff.contents().getInt(offset);
        BlockId blk = buff.block();
        return SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval);
    }

    /**
     * Write a setstring record to the log and return its lsn.
     *
     * @param buff   the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     */
    public int setString(Buffer buff, int offset, String newval) {
        String oldval = buff.contents().getString(offset);
        BlockId blk = buff.block();
        return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);
    }

    /**
     * Rollback the transaction, by iterating
     * through the log records until it finds
     * the transaction's START record,
     * calling undo() for each of the transaction's
     * log records.
     */
    private void doRollback() {
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            if (rec.txNumber() == txnum) {
                if (rec.op() == START) return;
                rec.undo(tx);
            }
        }
    }

    /**
     * Do a complete database recovery.
     * The method iterates through the log records.
     * Whenever it finds a log record for an unfinished
     * transaction, it calls undo() on that record.
     * The method stops when it encounters a CHECKPOINT record
     * or the end of the log.
     */
    private void doRecover() {
        Collection<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            if (rec.op() == CHECKPOINT) return;
            if (rec.op() == COMMIT || rec.op() == ROLLBACK) finishedTxs.add(rec.txNumber());
            else if (!finishedTxs.contains(rec.txNumber())) rec.undo(tx);
        }
    }
}
