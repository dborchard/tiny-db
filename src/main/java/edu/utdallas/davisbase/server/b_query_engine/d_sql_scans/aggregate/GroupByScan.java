package edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.aggregate;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.aggregate.domain.GroupValue;
import edu.utdallas.davisbase.server.b_query_engine.e_sql_materialize.AggregationFn;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;

import java.util.List;

/**
 * The Scan class for the <i>groupby</i> operator.
 *
 * @author Edward Sciore
 */
public class GroupByScan implements Scan {
    private Scan s;
    private List<String> groupfields;
    private List<AggregationFn> aggfns;
    private GroupValue groupval;
    private boolean moregroups;

    /**
     * Create a groupby scan, given a grouped table scan.
     *
     * @param s           the grouped scan
     * @param groupfields the group fields
     * @param aggfns      the aggregation functions
     */
    public GroupByScan(Scan s, List<String> groupfields, List<AggregationFn> aggfns) {
        this.s = s;
        this.groupfields = groupfields;
        this.aggfns = aggfns;
        seekToHead_Query();
    }

    /**
     * Position the scan before the first group.
     * Internally, the underlying scan is always
     * positioned at the first record of a group, which
     * means that this method moves to the
     * first underlying record.
     *
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void seekToHead_Query() {
        s.seekToHead_Query();
        moregroups = s.next();
    }

    /**
     * Move to the next group.
     * The key of the group is determined by the
     * group values at the current record.
     * The method repeatedly reads underlying records until
     * it encounters a record having a different key.
     * The aggregation functions are called for each record
     * in the group.
     * The values of the grouping fields for the group are saved.
     *
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        if (!moregroups) return false;
        for (AggregationFn fn : aggfns)
            fn.processFirst(s);
        groupval = new GroupValue(s, groupfields);
        while (moregroups = s.next()) {
            GroupValue gv = new GroupValue(s, groupfields);
            if (!groupval.equals(gv)) break;
            for (AggregationFn fn : aggfns)
                fn.processNext(s);
        }
        return true;
    }

    /**
     * Close the scan by closing the underlying scan.
     *
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        s.close();
    }

    /**
     * Get the Constant value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     *
     * @see simpledb.query.Scan#getVal(String)
     */
    public D_Constant getVal(String fldname) {
        if (groupfields.contains(fldname)) return groupval.getVal(fldname);
        for (AggregationFn fn : aggfns)
            if (fn.fieldName().equals(fldname)) return fn.value();
        throw new RuntimeException("field " + fldname + " not found.");
    }

    /**
     * Get the integer value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     *
     * @see simpledb.query.Scan#getVal(String)
     */
    public int getInt(String fldname) {
        return getVal(fldname).asInt();
    }

    /**
     * Get the string value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     *
     * @see simpledb.query.Scan#getVal(String)
     */
    public String getString(String fldname) {
        return getVal(fldname).asString();
    }

    /**
     * Return true if the specified field is either a
     * grouping field or created by an aggregation function.
     *
     * @see simpledb.query.Scan#hasField(String)
     */
    public boolean hasField(String fldname) {
        if (groupfields.contains(fldname)) return true;
        for (AggregationFn fn : aggfns)
            if (fn.fieldName().equals(fldname)) return true;
        return false;
    }
}

