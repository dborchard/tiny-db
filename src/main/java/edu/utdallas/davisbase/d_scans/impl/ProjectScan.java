package edu.utdallas.davisbase.d_scans.impl;

import edu.utdallas.davisbase.d_scans.Scan;
import edu.utdallas.davisbase.d_scans.domains.Constant;

import java.util.List;

/**
 * The scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * All methods except hasField delegate their work to the
 * underlying scan.
 * @author Edward Sciore
 */
public class ProjectScan implements Scan {
   private Scan s;
   private List<String> fieldlist;
   
   /**
    * Create a project scan having the specified
    * underlying scan and field list.
    * @param s the underlying scan
    * @param fieldlist the list of field names
    */
   public ProjectScan(Scan s, List<String> fieldlist) {
      this.s = s;
      this.fieldlist = fieldlist;
   }
   
   public void beforeFirst() {
      s.beforeFirst();
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
   
   public Constant getVal(String fldname) {
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
