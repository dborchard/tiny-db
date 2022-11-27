package edu.utdallas.davisbase.server.b_query_engine.e_sql_materialize.impl;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.b_query_engine.e_sql_materialize.AggregationFn;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;

/**
 * The <i>count</i> aggregation function.
 *
 * @author Edward Sciore
 */
public class CountFn implements AggregationFn {
    private String fldname;
    private int count;

    public CountFn(String fldname) {
        this.fldname = fldname;
    }


    public void processFirst(Scan s) {
        count = 1;
    }


    public void processNext(Scan s) {
        count++;
    }


    public String fieldName() {
        return "countof" + fldname;
    }


    public D_Constant value() {
        return new D_Constant(count);
    }
}
