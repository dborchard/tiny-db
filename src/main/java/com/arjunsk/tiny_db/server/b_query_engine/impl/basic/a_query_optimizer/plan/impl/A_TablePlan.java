package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl;

import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.MetadataMgr;
import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.stats.domain.StatInfo;
import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;
import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.table.TablePhysicalLayout;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import com.arjunsk.tiny_db.server.d_storage_engine.RORecordScan;
import com.arjunsk.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import com.arjunsk.tiny_db.server.d_storage_engine.impl.data.heap.HeapRWRecordScan;

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
