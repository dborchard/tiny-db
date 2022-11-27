package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.regular;

import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular.ProjectScan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;

import java.util.List;


public class ProjectPlan implements Plan {
    private Plan p;
    private RecordValueSchema recordValueSchema = new RecordValueSchema();


    public ProjectPlan(Plan p, List<String> fieldlist) {
        this.p = p;
        for (String fldname : fieldlist)
            recordValueSchema.add(fldname, p.schema());
    }


    public Scan open() {
        Scan s = p.open();
        return new ProjectScan(s, recordValueSchema.fields());
    }

    public RecordValueSchema schema() {
        return recordValueSchema;
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }


    /**
     * Estimates the number of output records in the projection,
     * which is the same as in the underlying query.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Estimates the number of distinct field values
     * in the projection,
     * which is the same as in the underlying query.
     *
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

}
