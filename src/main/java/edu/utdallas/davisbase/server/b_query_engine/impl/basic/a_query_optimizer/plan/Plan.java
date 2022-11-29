package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan;

import edu.utdallas.davisbase.server.d_storage_engine.common.scans.RScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordValueSchema;


public interface Plan {

    RScan open();

    RecordValueSchema schema();

    int blocksAccessed();
}
