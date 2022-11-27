package edu.utdallas.davisbase.server.b_query_engine.e_sql_materialize;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;

/**
 * The interface implemented by aggregation functions.
 * Aggregation functions are used by the <i>groupby</i> operator.
 *
 * @author Edward Sciore
 */
public interface AggregationFn {

    /**
     * Use the current record of the specified scan
     * to be the first record in the group.
     *
     * @param s the scan to aggregate over.
     */
    void processFirst(Scan s);

    /**
     * Use the current record of the specified scan
     * to be the next record in the group.
     *
     * @param s the scan to aggregate over.
     */
    void processNext(Scan s);

    /**
     * Return the name of the new aggregation field.
     *
     * @return the name of the new aggregation field
     */
    String fieldName();

    /**
     * Return the computed aggregation value.
     *
     * @return the computed aggregation value
     */
    D_Constant value();
}
