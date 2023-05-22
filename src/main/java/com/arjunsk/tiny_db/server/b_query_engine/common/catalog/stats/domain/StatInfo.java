package com.arjunsk.tiny_db.server.b_query_engine.common.catalog.stats.domain;

/**
 * A StatInfo object holds three pieces of
 * statistical information about a table:
 * the number of blocks, the number of records,
 * and the number of distinct values for each field.
 *
 * @author Edward Sciore
 */
public class StatInfo {
    private int numBlocks;
    private int numRecs;

    /**
     * Create a StatInfo object.
     * Note that the number of distinct values is not
     * passed into the constructor.
     * The object fakes this value.
     *
     * @param numblocks the number of blocks in the table
     * @param numrecs   the number of records in the table
     */
    public StatInfo(int numblocks, int numrecs) {
        this.numBlocks = numblocks;
        this.numRecs = numrecs;
    }

    /**
     * Return the estimated number of blocks in the table.
     *
     * @return the estimated number of blocks in the table
     */
    public int blocksAccessed() {
        return numBlocks;
    }

    /**
     * Return the estimated number of records in the table.
     *
     * @return the estimated number of records in the table
     */
    public int recordsOutput() {
        return numRecs;
    }

    /**
     * Return the estimated number of distinct values
     * for the specified field.
     * This estimate is a complete guess, because doing something
     * reasonable is beyond the scope of this system.
     *
     * @param fldname the name of the field
     * @return a guess as to the number of distinct field values
     */
    public int distinctValues(String fldname) {
        return 1 + (numRecs / 3);
    }
}
