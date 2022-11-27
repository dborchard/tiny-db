package edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.query_engine.c_sql_scans.ProjectScan;
import edu.utdallas.davisbase.server.storage_engine.a_scans.Scan;
import edu.utdallas.davisbase.server.storage_engine.b_io.data.heap.TableSchema;

import java.util.List;


public class ProjectPlan implements Plan {
    private Plan p;
    private TableSchema tableSchema = new TableSchema();


    public ProjectPlan(Plan p, List<String> fieldlist) {
        this.p = p;
        for (String fldname : fieldlist)
            tableSchema.add(fldname, p.schema());
    }


    public Scan open() {
        Scan s = p.open();
        return new ProjectScan(s, tableSchema.fields());
    }

    public TableSchema schema() {
        return tableSchema;
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }
}
