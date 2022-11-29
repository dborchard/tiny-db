package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.d_sql_scans.ProjectScan;
import edu.utdallas.davisbase.server.d_storage_engine.common.scans.RScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordValueSchema;

import java.util.List;


public class ProjectPlan implements Plan {
    private Plan p;
    private RecordValueSchema recordValueSchema = new RecordValueSchema();


    public ProjectPlan(Plan p, List<String> fieldlist) {
        this.p = p;
        for (String fldname : fieldlist)
            recordValueSchema.add(fldname, p.schema());
    }


    public RScan open() {
        RScan s = p.open();
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
