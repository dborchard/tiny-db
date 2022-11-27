package edu.utdallas.davisbase.db.query_engine.a_planner.plan;

import edu.utdallas.davisbase.db.query_engine.c_scans.Scan;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableSchema;


public interface Plan {

    Scan open();

    TableSchema schema();
}
