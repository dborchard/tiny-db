package edu.utdallas.tiny_db.server.a_frontend.common.domain.clause;

import edu.utdallas.tiny_db.server.d_storage_engine.RORecordScan;

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
    public boolean isSatisfied(RORecordScan s) {
        for (B_Term t : terms)
            if (!t.isSatisfied(s)) return false;
        return true;
    }


    public D_Constant equatesWithConstant(String fldname) {
        for (B_Term t : terms) {
            D_Constant c = t.equatesWithConstant(fldname);
            if (c != null)
                return c;
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
