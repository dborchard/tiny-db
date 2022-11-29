package edu.utdallas.davisbase.server.c_key_value_store;

import edu.utdallas.davisbase.server.c_key_value_store.b_buffer_mgr.BufferMgr;
import edu.utdallas.davisbase.server.d_storage_engine.common.file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.common.file.FileMgr;
import edu.utdallas.davisbase.server.d_storage_engine.common.file.Page;


/**
 * Provide transaction management for clients,
 * ensuring that all transactions are serializable, recoverable,
 * and in general satisfy the ACID properties.
 *
 * @author Edward Sciore
 */
public class Transaction {
    private static final int END_OF_FILE = -1;
    private static int nextTxNum = 0;
    private FileMgr fm;
    private BufferMgr bm;
    private int txnum;


    public Transaction(FileMgr fm) {
        this.fm = fm;
        txnum = nextTxNumber();
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        return nextTxNum;
    }

    public void commit() {
    }

    public void rollback() {
    }

    public void recover() {

    }

    /**
     * Pin the specified block.
     * The transaction manages the buffer for the client.
     *
     * @param blk a reference to the disk block
     */
    public void pin(BlockId blk) {
    }

    /**
     * Unpin the specified block.
     * The transaction looks up the buffer pinned to this block,
     * and unpins it.
     *
     * @param blk a reference to the disk block
     */
    public void unpin(BlockId blk) {
    }

    public synchronized int getInt(BlockId blk, int offset) {
        Page contents = new Page(fm.blockSize());
        fm.read(blk, contents);
        return contents.getInt(offset);
    }

    public synchronized String getString(BlockId blk, int offset) {
        Page contents = new Page(fm.blockSize());
        fm.read(blk, contents);
        return contents.getString(offset);
    }

    /**
     * Store an integer at the specified offset
     * of the specified block.
     * The method first obtains an XLock on the block.
     * It then reads the current value at that offset,
     * puts it into an update log record, and
     * writes that record to the log.
     * Finally, it calls the buffer to store the value,
     * passing in the LSN of the log record and the transaction's id.
     *
     * @param blk    a reference to the disk block
     * @param offset a byte offset within that block
     * @param val    the value to be stored
     */
    public synchronized void setInt(BlockId blk, int offset, int val) {
        Page contents = new Page(fm.blockSize());
        fm.read(blk, contents);

        contents.setInt(offset, val);
        fm.write(blk, contents);
    }

    /**
     * Store a string at the specified offset
     * of the specified block.
     * The method first obtains an XLock on the block.
     * It then reads the current value at that offset,
     * puts it into an update log record, and
     * writes that record to the log.
     * Finally, it calls the buffer to store the value,
     * passing in the LSN of the log record and the transaction's id.
     *
     * @param blk    a reference to the disk block
     * @param offset a byte offset within that block
     * @param val    the value to be stored
     */
    public synchronized void setString(BlockId blk, int offset, String val) {
        Page contents = new Page(fm.blockSize());
        fm.read(blk, contents);

        contents.setString(offset, val);
        fm.write(blk, contents);
    }

    /**
     * Return the number of blocks in the specified file.
     * This method first obtains an SLock on the
     * "end of the file", before asking the file manager
     * to return the file size.
     *
     * @param filename the name of the file
     * @return the number of blocks in the file
     */
    public synchronized int size(String filename) {
        return fm.length(filename);
    }

    /**
     * Append a new block to the end of the specified file
     * and returns a reference to it.
     * This method first obtains an XLock on the
     * "end of the file", before performing the append.
     *
     * @param filename the name of the file
     * @return a reference to the newly-created disk block
     */
    public synchronized BlockId append(String filename) {
        return fm.append(filename);
    }

    public synchronized int blockSize() {
        return fm.blockSize();
    }
}
