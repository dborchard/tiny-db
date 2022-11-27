package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.regular;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular.SelectScan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;

/**
 * The Plan class corresponding to the <i>select</i>
 * relational algebra operator.
 *
 * @author Edward Sciore
 */
public class SelectPlan implements Plan {
    private Plan p;
    private A_Predicate pred;

    public SelectPlan(Plan p, A_Predicate pred) {
        this.p = p;
        this.pred = pred;
    }

    public Scan open() {
        Scan s = p.open();
        return new SelectScan(s, pred);
    }


    public RecordValueSchema schema() {
        return p.schema();
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    /**
     * Estimates the number of output records in the selection,
     * which is determined by the
     * reduction factor of the predicate.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput() / pred.reductionFactor(p);
    }

    /**
     * Estimates the number of distinct field values
     * in the projection.
     * If the predicate contains a term equating the specified
     * field to a constant, then this value will be 1.
     * Otherwise, it will be the number of the distinct values
     * in the underlying query
     * (but not more than the size of the output table).
     *
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (pred.equatesWithConstant(fldname) != null)
            return 1;
        else {
            String fldname2 = pred.equatesWithField(fldname);
            if (fldname2 != null)
                return Math.min(p.distinctValues(fldname),
                        p.distinctValues(fldname2));
            else
                return p.distinctValues(fldname);
        }
    }
}
