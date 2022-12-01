package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import edu.utdallas.davisbase.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.d_storage_engine.RORecordScan;
import edu.utdallas.davisbase.server.d_storage_engine.common.transaction.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.heap.HeapRWRecordScan;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.table.TablePhysicalLayout;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.table.TableDefinition;

/**
 * The Plan class corresponding to a table.
 *
 * @author Edward Sciore
 */
public class A_TablePlan implements Plan {

  private String tblname;
  private Transaction tx;
  private TablePhysicalLayout recordValueLayout;
  private StatInfo si;

  public A_TablePlan(Transaction tx, String tblname, MetadataMgr md) {
    this.tblname = tblname;
    this.tx = tx;
    recordValueLayout = md.getLayout(tblname, tx);
  }


  public RORecordScan open() {

    // NOTE: The Place where Query Engine interacts with StorageEngine
    return new HeapRWRecordScan(tx, tblname, recordValueLayout);
  }


  public TableDefinition schema() {
    return recordValueLayout.schema();
  }

  @Override
  public int blocksAccessed() {
    return si.blocksAccessed();
  }
}
