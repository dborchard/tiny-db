package edu.utdallas.davisbase.server.c_key_value_store.a_transaction.a_concurrency;

import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;

import java.util.HashMap;
import java.util.Map;

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then that transaction is placed on a wait list.
 * There is only one wait list for all blocks.
 * When the last lock on a block is unlocked, then all transactions
 * are removed from the wait list and rescheduled.
 * If one of those transactions discovers that the lock it is waiting for
 * is still locked, it will place itself back on the wait list.
 *
 * @author Edward Sciore
 */
class LockTable {
    private static final long MAX_TIME = 10000; // 10 seconds

    private Map<BlockId, Integer> locks = new HashMap<BlockId, Integer>();

    /**
     * Grant an SLock on the specified block.
     * If an XLock exists when the method is called,
     * then the calling thread will be placed on a wait list
     * until the lock is released.
     * If the thread remains on the wait list for a certain
     * amount of time (currently 10 seconds),
     * then an exception is thrown.
     *
     * @param blk a reference to the disk block
     */
    public synchronized void sLock(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXlock(blk) && !waitingTooLong(timestamp))
                wait(MAX_TIME);
            if (hasXlock(blk))
                throw new LockAbortException();
            int val = getLockVal(blk);  // will not be negative
            locks.put(blk, val + 1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * Grant an XLock on the specified block.
     * If a lock of any type exists when the method is called,
     * then the calling thread will be placed on a wait list
     * until the locks are released.
     * If the thread remains on the wait list for a certain
     * amount of time (currently 10 seconds),
     * then an exception is thrown.
     *
     * @param blk a reference to the disk block
     */
    synchronized void xLock(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSLocks(blk) && !waitingTooLong(timestamp))
                wait(MAX_TIME);
            if (hasOtherSLocks(blk))
                throw new LockAbortException();
            locks.put(blk, -1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * Release a lock on the specified block.
     * If this lock is the last lock on that block,
     * then the waiting transactions are notified.
     *
     * @param blk a reference to the disk block
     */
    synchronized void unlock(BlockId blk) {
        int val = getLockVal(blk);
        if (val > 1)
            locks.put(blk, val - 1);
        else {
            locks.remove(blk);
            notifyAll();
        }
    }

    private boolean hasXlock(BlockId blk) {
        return getLockVal(blk) < 0;
    }

    private boolean hasOtherSLocks(BlockId blk) {
        return getLockVal(blk) > 1;
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

    private int getLockVal(BlockId blk) {
        Integer ival = locks.get(blk);
        return (ival == null) ? 0 : ival.intValue();
    }
}
