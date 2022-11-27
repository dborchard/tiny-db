package edu.utdallas.davisbase.server.a_frontend.domain.commands;

import edu.utdallas.davisbase.server.a_frontend.domain.clause.A_Predicate;

import java.util.List;

/**
 * Data for the SQL <i>select</i> statement.
 *
 * @author Edward Sciore
 */
public class QueryData {
    private List<String> fields;
    private String table;
    private A_Predicate pred;

    /**
     * Saves the field and table list and predicate.
     */
    public QueryData(List<String> fields, String table, A_Predicate pred) {
        this.fields = fields;
        this.table = table;
        this.pred = pred;
    }

    /**
     * Returns the fields mentioned in the select clause.
     *
     * @return a list of field names
     */
    public List<String> fields() {
        return fields;
    }

    /**
     * Returns the tables mentioned in the from clause.
     *
     * @return a collection of table names
     */
    public String table() {
        return table;
    }

    /**
     * Returns the predicate that describes which
     * records should be in the output table.
     *
     * @return the query predicate
     */
    public A_Predicate pred() {
        return pred;
    }

}
