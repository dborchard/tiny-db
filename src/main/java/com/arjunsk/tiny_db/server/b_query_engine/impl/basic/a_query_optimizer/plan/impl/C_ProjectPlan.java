package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.b_execution_engine.C_Project_RORecordScan;
import com.arjunsk.tiny_db.server.d_storage_engine.RORecordScan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;

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
