package edu.utdallas.davisbase.server.a_frontend.common.domain.clause;

import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A predicate is a Boolean combination of terms.
 *
 * @author Edward Sciore
 */
public class A_Predicate {
    private List<B_Term> terms = new ArrayList<B_Term>();

    /**
     * Create an empty predicate, corresponding to "true".
     */
    public A_Predicate() {
    }

    /**
     * Create a predicate containing a single term.
     *
     * @param t the term
     */
    public A_Predicate(B_Term t) {
        terms.add(t);
    }

    /**
     * Modifies the predicate to be the conjunction of
     * itself and the specified predicate.
     *
     * @param pred the other predicate
     */
    public void conjoinWith(A_Predicate pred) {
        terms.addAll(pred.terms);
    }

    /**
     * Returns true if the predicate evaluates to true
     * with respect to the specified scan.
     *
     * @param s the scan
     * @return true if the predicate is true in the scan
     */
    public boolean isSatisfied(Scan s) {
        for (B_Term t : terms)
            if (!t.isSatisfied(s)) return false;
        return true;
    }

    /**
     * Calculate the extent to which selecting on the predicate
     * reduces the number of records output by a query.
     * For example if the reduction factor is 2, then the
     * predicate cuts the size of the output in half.
     *
     * @param p the query's plan
     * @return the integer reduction factor.
     */
    public int reductionFactor(Plan p) {
        int factor = 1;
        for (B_Term t : terms)
            factor *= t.reductionFactor(p);
        return factor;
    }

    /**
     * Return the subpredicate that applies to the specified schema.
     *
     * @param sch the schema
     * @return the subpredicate applying to the schema
     */
    public A_Predicate selectSubPred(RecordValueSchema sch) {
        A_Predicate result = new A_Predicate();
        for (B_Term t : terms)
            if (t.appliesTo(sch)) result.terms.add(t);
        if (result.terms.size() == 0) return null;
        else return result;
    }

    /**
     * Return the subpredicate consisting of terms that apply
     * to the union of the two specified schemas,
     * but not to either schema separately.
     *
     * @param sch1 the first schema
     * @param sch2 the second schema
     * @return the subpredicate whose terms apply to the union of the two schemas but not either schema separately.
     */
    public A_Predicate joinSubPred(RecordValueSchema sch1, RecordValueSchema sch2) {
        A_Predicate result = new A_Predicate();
        RecordValueSchema newsch = new RecordValueSchema();
        newsch.addAll(sch1);
        newsch.addAll(sch2);
        for (B_Term t : terms)
            if (!t.appliesTo(sch1) && !t.appliesTo(sch2) && t.appliesTo(newsch)) result.terms.add(t);
        if (result.terms.size() == 0) return null;
        else return result;
    }

    /**
     * Determine if there is a term of the form "F=c"
     * where F is the specified field and c is some constant.
     * If so, the method returns that constant.
     * If not, the method returns null.
     *
     * @param fldname the name of the field
     * @return either the constant or null
     */
    public D_Constant equatesWithConstant(String fldname) {
        for (B_Term t : terms) {
            D_Constant c = t.equatesWithConstant(fldname);
            if (c != null) return c;
        }
        return null;
    }

    /**
     * Determine if there is a term of the form "F1=F2"
     * where F1 is the specified field and F2 is another field.
     * If so, the method returns the name of that field.
     * If not, the method returns null.
     *
     * @param fldname the name of the field
     * @return the name of the other field, or null
     */
    public String equatesWithField(String fldname) {
        for (B_Term t : terms) {
            String s = t.equatesWithField(fldname);
            if (s != null) return s;
        }
        return null;
    }

    public String toString() {
        Iterator<B_Term> iter = terms.iterator();
        if (!iter.hasNext()) return "";
        String result = iter.next().toString();
        while (iter.hasNext()) result += " and " + iter.next().toString();
        return result;
    }
}
