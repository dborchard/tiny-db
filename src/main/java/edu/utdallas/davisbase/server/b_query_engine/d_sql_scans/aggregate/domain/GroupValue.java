package edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.aggregate.domain;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An object that holds the values of the grouping fields
 * for the current record of a scan.
 *
 * @author Edward Sciore
 */
public class GroupValue {
    private Map<String, D_Constant> vals = new HashMap<>();

    /**
     * Create a new group value, given the specified scan
     * and list of fields.
     * The values in the current record of each field are
     * stored.
     *
     * @param s      a scan
     * @param fields the list of fields
     */
    public GroupValue(Scan s, List<String> fields) {
        vals = new HashMap<String, D_Constant>();
        for (String fldname : fields)
            vals.put(fldname, s.getVal(fldname));
    }

    /**
     * Return the Constant value of the specified field in the group.
     *
     * @param fldname the name of a field
     * @return the value of the field in the group
     */
    public D_Constant getVal(String fldname) {
        return vals.get(fldname);
    }

    /**
     * Two GroupValue objects are equal if they have the same values
     * for their grouping fields.
     *
     * @see Object#equals(Object)
     */
    public boolean equals(Object obj) {
        GroupValue gv = (GroupValue) obj;
        for (String fldname : vals.keySet()) {
            D_Constant v1 = vals.get(fldname);
            D_Constant v2 = gv.getVal(fldname);
            if (!v1.equals(v2)) return false;
        }
        return true;
    }

    /**
     * The hashcode of a GroupValue object is the sum of the
     * hashcodes of its field values.
     *
     * @see Object#hashCode()
     */
    public int hashCode() {
        int hashval = 0;
        for (D_Constant c : vals.values())
            hashval += c.hashCode();
        return hashval;
    }
}