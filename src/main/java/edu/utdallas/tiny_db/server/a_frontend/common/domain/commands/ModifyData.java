package edu.utdallas.tiny_db.server.a_frontend.common.domain.commands;

import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.C_Expression;
import lombok.ToString;

/**
 * Data for the SQL <i>update</i> statement.
 *
 * @author Edward Sciore
 */
@ToString
public class ModifyData {
    private String tblname;
    private String fldname;
    private C_Expression newval;
    private A_Predicate pred;

    /**
     * Saves the table name, the modified field and its new value, and the predicate.
     */
    public ModifyData(String tblname, String fldname, C_Expression newval, A_Predicate pred) {
        this.tblname = tblname;
        this.fldname = fldname;
        this.newval = newval;
        this.pred = pred;
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
     * Returns the field whose values will be modified
     *
     * @return the name of the target field
     */
    public String targetField() {
        return fldname;
    }

    /**
     * Returns an expression.
     * Evaluating this expression for a record produces
     * the value that will be stored in the record's target field.
     *
     * @return the target expression
     */
    public C_Expression newValue() {
        return newval;
    }

    /**
     * Returns the predicate that describes which
     * records should be modified.
     *
     * @return the modification predicate
     */
    public A_Predicate pred() {
        return pred;
    }
}