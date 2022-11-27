package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan;

import edu.utdallas.davisbase.server.d_storage_engine.common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.file_organization.heap.TableSchema;


public interface Plan {

    Scan open();

    TableSchema schema();

    int blocksAccessed();
}
