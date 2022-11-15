package edu.utdallas.davisbase.b0_planner.plan;

import edu.utdallas.davisbase.d_scans.Scan;
import edu.utdallas.davisbase.e_record.Schema;


public interface Plan {


    Scan open();

    Schema schema();
}
