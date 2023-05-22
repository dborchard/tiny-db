package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan;

import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;
import com.arjunsk.tiny_db.server.d_storage_engine.RORecordScan;

/**
 * Plan encapsulate relational algebra and cost of operation.
 * This is passed on to Planner for getting an efficient physical plan.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public interface Plan {

    RORecordScan open();

    TableDefinition schema();

    int blocksAccessed();
}
