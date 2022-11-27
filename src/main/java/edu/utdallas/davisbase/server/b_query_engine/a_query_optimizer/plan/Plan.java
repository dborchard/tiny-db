package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan;

import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;


public interface Plan {

    Scan open();

    RecordValueSchema schema();

    int blocksAccessed();


    /**
     * Returns an estimate of the number of records
     * in the query's output table.
     *
     * @return the estimated number of output records
     */
    public int recordsOutput();

    /**
     * Returns an estimate of the number of distinct values
     * for the specified field in the query's output table.
     *
     * @param fldname the name of a field
     * @return the estimated number of distinct field values in the output
     */
    public int distinctValues(String fldname);
}
