package edu.utdallas.tiny_db.server.b_query_engine.impl.basic.b_sql_scans;

import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.commands.ModifyData;
import edu.utdallas.tiny_db.server.d_storage_engine.RWRecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.RORecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.page.RecordKey;

/**
 * The scan class corresponding to the <i>select</i> relational algebra operator. All methods except
 * next delegate their work to the underlying scan.
 *
 * @author Edward Sciore
 */
public class A_Select_RWRecordScan implements RWRecordScan {

  private RORecordScan s;
  private A_Predicate pred;

  /**
   * Create a select scan having the specified underlying scan and predicate.
   *
   * @param s    the scan of the underlying query
   * @param pred the selection predicate
   */
  public A_Select_RWRecordScan(RORecordScan s, A_Predicate pred) {
    this.s = s;
    this.pred = pred;
  }

  // Scan methods

  public void seekToQueryStart() {
    s.seekToQueryStart();
  }

  public boolean next() {
    while (s.next()) {
      if (pred.isSatisfied(s)) {
        return true;
      }
    }
    return false;
  }

  public int getInt(String fldname) {
    return s.getInt(fldname);
  }

  public String getString(String fldname) {
    return s.getString(fldname);
  }

  public D_Constant getVal(String fldname) {
    return s.getVal(fldname);
  }

  public boolean hasField(String fldname) {
    return s.hasField(fldname);
  }

  public void close() {
    s.close();
  }

  //--------------- RWDataRecordScan Methods

  /**
   * Usage
   * {@link
   * edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.b_rule_base.BetterUpdatePlanner#executeModify(ModifyData,
   * Transaction)}
   */


  public void setInt(String fldname, int val) {
    RWRecordScan us = (RWRecordScan) s;
    us.setInt(fldname, val);
  }

  public void setString(String fldname, String val) {
    RWRecordScan us = (RWRecordScan) s;
    us.setString(fldname, val);
  }

  public void setVal(String fldname, D_Constant val) {
    RWRecordScan us = (RWRecordScan) s;
    us.setVal(fldname, val);
  }

  public void delete() {
    RWRecordScan us = (RWRecordScan) s;
    us.delete();
  }

  public void seekToInsertStart() {
    RWRecordScan us = (RWRecordScan) s;
    us.seekToInsertStart();
  }

  public RecordKey getRid() {
    RWRecordScan us = (RWRecordScan) s;
    return us.getRid();
  }

  public void seekTo(RecordKey recordKey) {
    RWRecordScan us = (RWRecordScan) s;
    us.seekTo(recordKey);
  }
}
