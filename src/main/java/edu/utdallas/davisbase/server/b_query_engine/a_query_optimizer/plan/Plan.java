package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan;

import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.a_file_organization.heap.RecordValueSchema;


public interface Plan {

    Scan open();

    RecordValueSchema schema();

    int blocksAccessed();
}
