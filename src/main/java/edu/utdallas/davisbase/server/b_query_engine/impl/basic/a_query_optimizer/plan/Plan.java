package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan;

import edu.utdallas.davisbase.server.d_storage_engine.common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.heap.RecordValueSchema;


public interface Plan {

    Scan open();

    RecordValueSchema schema();

    int blocksAccessed();
}
