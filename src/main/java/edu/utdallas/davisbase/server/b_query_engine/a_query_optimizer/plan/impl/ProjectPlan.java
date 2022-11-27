package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.ProjectScan;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;

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
}
