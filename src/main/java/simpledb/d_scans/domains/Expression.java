package simpledb.d_scans.domains;

import simpledb.d_scans.Scan;
import simpledb.e_record.Schema;

/**
 * The interface corresponding to SQL expressions.
 * @author Edward Sciore
 *
 */
public class Expression {
   private Constant val = null;
   private String fldname = null;
   
   public Expression(Constant val) {
      this.val = val;
   }
   
   public Expression(String fldname) {
      this.fldname = fldname;
   }
   
   /**
    * Evaluate the expression with respect to the
    * current record of the specified scan.
    * @param s the scan
    * @return the value of the expression, as a Constant
    */
   public Constant evaluate(Scan s) {
      return (val != null) ? val : s.getVal(fldname);
   }
   
   /**
    * Return true if the expression is a field reference.
    * @return true if the expression denotes a field
    */
   public boolean isFieldName() {
      return fldname != null;
   }
   
   /**
    * Return the constant corresponding to a constant expression,
    * or null if the expression does not
    * denote a constant.
    * @return the expression as a constant
    */
   public Constant asConstant() {
      return val;
   }
   
   /**
    * Return the field name corresponding to a constant expression,
    * or null if the expression does not
    * denote a field.
    * @return the expression as a field name
    */
   public String asFieldName() {
      return fldname;
   }
   
   /**
    * Determine if all of the fields mentioned in this expression
    * are contained in the specified schema.
    * @param sch the schema
    * @return true if all fields in the expression are in the schema
    */
   public boolean appliesTo(Schema sch) {
      return (val != null) ? true : sch.hasField(fldname);
   }
   
   public String toString() {
      return (val != null) ? val.toString() : fldname;
   }
}
