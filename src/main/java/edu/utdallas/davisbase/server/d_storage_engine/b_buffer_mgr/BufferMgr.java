package edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr;


import edu.utdallas.davisbase.server.d_storage_engine.a_disk.c_wal.LogMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 *
 * @author Edward Sciore
 */
public class BufferMgr {
    private Buffer[] bufferpool;
    private int numAvailable;
    private static final long MAX_TIME = 10000; // 10 seconds


    public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
        bufferpool = new Buffer[numbuffs];
        numAvailable = numbuffs;
        for (int i = 0; i < numbuffs; i++)
            bufferpool[i] = new Buffer(fm, lm);
    }

    /**
     * Returns the number of available (i.e. unpinned) buffers.
     *
     * @return the number of available buffers
     */
    public synchronized int available() {
        return numAvailable;
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     *
     * @param txnum the transaction's id number
     */
    public synchronized void flushAll(int txnum) {
        for (Buffer buff : bufferpool)
            if (buff.modifyingTx() == txnum)
                buff.flush();
    }


    /**
     * Unpins the specified data buffer. If its pin count
     * goes to zero, then notify any waiting threads.
     *
     * @param buff the buffer to be unpinned
     */
    public synchronized void unpin(Buffer buff) {
        buff.unpin();
        if (!buff.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }


    public synchronized Buffer pin(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buff = tryToPin(blk);
            while (buff == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buff = tryToPin(blk);
            }
            if (buff == null)
                throw new BufferAbortException();
            return buff;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

    /**
     * Tries to pin a buffer to the specified block.
     * If there is already a buffer assigned to that block
     * then that buffer is used;
     * otherwise, an unpinned buffer from the pool is chosen.
     * Returns a null value if there are no available buffers.
     *
     * @param blk a reference to a disk block
     * @return the pinned buffer
     */
    private Buffer tryToPin(BlockId blk) {
        Buffer buff = findExistingBuffer(blk);
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            if (buff == null)
                return null;
            buff.assignToBlock(blk);
        }
        if (!buff.isPinned())
            numAvailable--;
        buff.pin();
        return buff;
    }

    private Buffer findExistingBuffer(BlockId blk) {
        for (Buffer buff : bufferpool) {
            BlockId b = buff.block();
            if (b != null && b.equals(blk))
                return buff;
        }
        return null;
    }

    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buff : bufferpool)
            if (!buff.isPinned())
                return buff;
        return null;
    }
}
