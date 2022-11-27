package edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr.record.impl;

import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.c_key_value_store.a_transaction.c_recovery_mgr.record.LogRecord;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.c_wal.LogMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.Page;

/**
 * The CHECKPOINT log record.
 *
 * @author Edward Sciore
 */
public class CheckpointRecord implements LogRecord {
    public CheckpointRecord() {
    }

    /**
     * A static method to write a checkpoint record to the log.
     * This log record contains the CHECKPOINT operator,
     * and nothing else.
     *
     * @return the LSN of the last log value
     */
    public static int writeToLog(LogMgr lm) {
        byte[] rec = new byte[Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, CHECKPOINT);
        return lm.append(rec);
    }

    public int op() {
        return CHECKPOINT;
    }

    /**
     * Checkpoint records have no associated transaction,
     * and so the method returns a "dummy", negative txid.
     */
    public int txNumber() {
        return -1; // dummy value
    }

    /**
     * Does nothing, because a checkpoint record
     * contains no undo information.
     */
    public void undo(Transaction tx) {
    }

    public String toString() {
        return "<CHECKPOINT>";
    }
}
