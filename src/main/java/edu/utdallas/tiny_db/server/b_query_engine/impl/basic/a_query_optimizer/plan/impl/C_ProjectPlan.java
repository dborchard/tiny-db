package edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.c_sql_scans.C_Project_RORecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.RORecordScan;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;

import java.util.List;

/**
 * Projection - Subset of columns.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class C_ProjectPlan implements Plan {
    private Plan p;
    private TableDefinition tableDefinition = new TableDefinition();


    public C_ProjectPlan(Plan p, List<String> fieldlist) {
        this.p = p;
        for (String fldname : fieldlist)
            tableDefinition.add(fldname, p.schema());
    }


    public RORecordScan open() {
        RORecordScan s = p.open();
        return new C_Project_RORecordScan(s, tableDefinition.fields());
    }

    public TableDefinition schema() {
        return tableDefinition;
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }
}
