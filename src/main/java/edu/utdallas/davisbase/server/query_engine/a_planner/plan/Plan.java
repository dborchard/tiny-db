package edu.utdallas.davisbase.server.query_engine.a_planner.plan;

import edu.utdallas.davisbase.server.storage_engine.a_scans.Scan;
import edu.utdallas.davisbase.server.storage_engine.b_io.data.heap.TableSchema;


public interface Plan {

    Scan open();

    TableSchema schema();
}
