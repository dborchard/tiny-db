package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.b_execution_engine;

import com.arjunsk.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import com.arjunsk.tiny_db.server.d_storage_engine.RORecordScan;

import java.util.List;

/**
 * The scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * All methods except hasField delegate their work to the
 * underlying scan.
 *
 * @author Edward Sciore
 */
public class C_Project_RORecordScan implements RORecordScan {
    private final RORecordScan s;
    private final List<String> fieldlist;

    /**
     * Create a project scan having the specified
     * underlying scan and field list.
     *
     * @param s         the underlying scan
     * @param fieldlist the list of field names
     */
    public C_Project_RORecordScan(RORecordScan s, List<String> fieldlist) {
        this.s = s;
        this.fieldlist = fieldlist;
    }

    public void seekToQueryStart() {
        s.seekToQueryStart();
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
