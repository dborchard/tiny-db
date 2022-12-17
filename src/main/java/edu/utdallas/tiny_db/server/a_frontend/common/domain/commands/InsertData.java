package edu.utdallas.tiny_db.server.a_frontend.common.domain.commands;

import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import lombok.ToString;

import java.util.List;

/**
 * Data for the SQL <i>insert</i> statement.
 *
 * @author Edward Sciore
 */
@ToString
public class InsertData {
    private String tblname;
    private List<String> flds;
    private List<D_Constant> vals;

    /**
     * Saves the table name and the field and value lists.
     */
    public InsertData(String tblname, List<String> flds, List<D_Constant> vals) {
        this.tblname = tblname;
        this.flds = flds;
        this.vals = vals;
    }

    /**
     * Returns the name of the affected table.
     *
     * @return the name of the affected table
     */
    public String tableName() {
        return tblname;
    }

    /**
     * Returns a list of fields for which
     * values will be specified in the new record.
     *
     * @return a list of field names
     */
    public List<String> fields() {
        return flds;
    }

    /**
     * Returns a list of values for the specified fields.
     * There is a one-one correspondence between this
     * list of values and the list of fields.
     *
     * @return a list of Constant values.
     */
    public List<D_Constant> vals() {
        return vals;
    }
}

