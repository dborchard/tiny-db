package edu.utdallas.davisbase.query_engine.b0_planner.plan;

import edu.utdallas.davisbase.query_engine.d_scans.Scan;
import edu.utdallas.davisbase.storage_engine.e_record.Schema;


public interface Plan {


    Scan open();

    Schema schema();
}
