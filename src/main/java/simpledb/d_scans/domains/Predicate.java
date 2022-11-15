package simpledb.d_scans.domains;

import simpledb.d_scans.Scan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A predicate is a Boolean combination of terms.
 *
 * @author Edward Sciore
 */
public class Predicate {
    private List<Term> terms = new ArrayList<Term>();

    /**
     * Create an empty predicate, corresponding to "true".
     */
    public Predicate() {
    }

    /**
     * Create a predicate containing a single term.
     *
     * @param t the term
     */
    public Predicate(Term t) {
        terms.add(t);
    }

    /**
     * Modifies the predicate to be the conjunction of
     * itself and the specified predicate.
     *
     * @param pred the other predicate
     */
    public void conjoinWith(Predicate pred) {
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
        for (Term t : terms)
            if (!t.isSatisfied(s)) return false;
        return true;
    }


    public Constant equatesWithConstant(String fldname) {
        for (Term t : terms) {
            Constant c = t.equatesWithConstant(fldname);
            if (c != null)
                return c;
        }
        return null;
    }

    public String toString() {
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext()) return "";
        String result = iter.next().toString();
        while (iter.hasNext()) result += " and " + iter.next().toString();
        return result;
    }
}
