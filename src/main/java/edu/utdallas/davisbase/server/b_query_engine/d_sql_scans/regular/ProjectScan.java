package edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.Scan;

import java.util.List;

/**
 * The scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * All methods except hasField delegate their work to the
 * underlying scan.
 *
 * @author Edward Sciore
 */
public class ProjectScan implements Scan {
    private final Scan s;
    private final List<String> fieldlist;

    /**
     * Create a project scan having the specified
     * underlying scan and field list.
     *
     * @param s         the underlying scan
     * @param fieldlist the list of field names
     */
    public ProjectScan(Scan s, List<String> fieldlist) {
        this.s = s;
        this.fieldlist = fieldlist;
    }

    public void seekToHead_Query() {
        s.seekToHead_Query();
    }

    public boolean next() {
        return s.next();
    }

    public int getInt(String fldname) {
        if (hasField(fldname))
            return s.getInt(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public String getString(String fldname) {
        if (hasField(fldname))
            return s.getString(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public D_Constant getVal(String fldname) {
        if (hasField(fldname))
            return s.getVal(fldname);
        else
            throw new RuntimeException("field " + fldname + " not found.");
    }

    public boolean hasField(String fldname) {
        return fieldlist.contains(fldname);
    }

    public void close() {
        s.close();
    }
}
