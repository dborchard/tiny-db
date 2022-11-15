package simpledb.b0_planner.plan;

import simpledb.d_scans.Scan;
import simpledb.e_record.Schema;


public interface Plan {


    Scan open();

    Schema schema();
}
