package edu.utdallas.davisbase.server.d_storage_engine.impl.b_index.btree.common;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;

/**
 * A directory entry has two components: the number of the child block,
 * and the dataval of the first record in that block.
 *
 * @author Edward Sciore
 */
public class DirEntry {
    private D_Constant dataval;
    private int blocknum;

    /**
     * Creates a new entry for the specified dataval and block number.
     *
     * @param dataval  the dataval
     * @param blocknum the block number
     */
    public DirEntry(D_Constant dataval, int blocknum) {
        this.dataval = dataval;
        this.blocknum = blocknum;
    }

    /**
     * Returns the dataval component of the entry
     *
     * @return the dataval component of the entry
     */
    public D_Constant dataVal() {
        return dataval;
    }

    /**
     * Returns the block number component of the entry
     *
     * @return the block number component of the entry
     */
    public int blockNumber() {
        return blocknum;
    }
}

