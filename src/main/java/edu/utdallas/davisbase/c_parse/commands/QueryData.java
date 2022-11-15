package edu.utdallas.davisbase.c_parse.commands;

import edu.utdallas.davisbase.d_scans.domains.Predicate;

import java.util.List;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private String table;
   private Predicate pred;
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, String table, Predicate pred) {
      this.fields = fields;
      this.table = table;
      this.pred = pred;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public String table() {
      return table;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

}
