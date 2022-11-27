package edu.utdallas.davisbase.server.a_frontend.common.domain.clause;

import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;

/**
 * A term is a comparison between two expressions.
 *
 * @author Edward Sciore
 */
public class B_Term {
    private C_Expression lhs, rhs;

    /**
     * Create a new term that compares two expressions
     * for equality.
     *
     * @param lhs the LHS expression
     * @param rhs the RHS expression
     */
    public B_Term(C_Expression lhs, C_Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    /**
     * Return true if both of the term's expressions
     * evaluate to the same constant,
     * with respect to the specified scan.
     *
     * @param s the scan
     * @return true if both expressions have the same value in the scan
     */
    public boolean isSatisfied(Scan s) {
        D_Constant lhsval = lhs.evaluate(s);
        D_Constant rhsval = rhs.evaluate(s);
        return rhsval.equals(lhsval);
    }

    /**
     * Determine if this term is of the form "F=c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     *
     * @param fldname the name of the field
     * @return either the constant or null
     */
    public D_Constant equatesWithConstant(String fldname) {
        if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && !rhs.isFieldName()) return rhs.asConstant();
        else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && !lhs.isFieldName()) return lhs.asConstant();
        else return null;
    }

    /**
     * Determine if this term is of the form "F1=F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     *
     * @param fldname the name of the field
     * @return either the name of the other field, or null
     */
    public String equatesWithField(String fldname) {
        if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && rhs.isFieldName()) return rhs.asFieldName();
        else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && lhs.isFieldName()) return lhs.asFieldName();
        else return null;
    }

    /**
     * Return true if both of the term's expressions
     * apply to the specified schema.
     *
     * @param sch the schema
     * @return true if both expressions apply to the schema
     */
    public boolean appliesTo(RecordValueSchema sch) {
        return lhs.appliesTo(sch) && rhs.appliesTo(sch);
    }

    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }

    /**
     * Calculate the extent to which selecting on the term reduces
     * the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * term cuts the size of the output in half.
     *
     * @param p the query's plan
     * @return the integer reduction factor.
     */
    public int reductionFactor(Plan p) {
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
        }
        if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return p.distinctValues(lhsName);
        }
        if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return p.distinctValues(rhsName);
        }
        // otherwise, the term equates constants
        if (lhs.asConstant().equals(rhs.asConstant())) return 1;
        else return Integer.MAX_VALUE;
    }
}
