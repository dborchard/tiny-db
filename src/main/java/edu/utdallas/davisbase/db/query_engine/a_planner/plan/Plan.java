package edu.utdallas.davisbase.db.query_engine.a_planner.plan;

import edu.utdallas.davisbase.db.query_engine.d_scans.Scan;
import edu.utdallas.davisbase.db.query_engine.e_record.Schema;


public interface Plan {


    Scan open();

    Schema schema();
}
