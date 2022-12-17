package edu.utdallas.tiny_db.server.b_query_engine.impl.calcite.core;

import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TablePhysicalLayout;
import edu.utdallas.tiny_db.server.d_storage_engine.RWRecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.HeapRWRecordScan;
import org.apache.calcite.linq4j.Enumerator;

/**
 * Enumerator connects with Catalog and StorageEngine ReadWrite Iterator, to create a calcite
 * readable iterator
 *
 * @author Arjun Sunil Kumar
 */
class A_Enumerator implements Enumerator<Object[]> {

  private final RWRecordScan scan;
  private final TablePhysicalLayout layout;

  A_Enumerator(MetadataMgr mdm, Transaction tx, String tableName) {
    layout = mdm.getLayout(tableName, tx);
    this.scan = new HeapRWRecordScan(tx, tableName, layout);
  }

  @Override
  public Object[] current() {
    return layout.schema().fields().stream().map(this.scan::getVal)
        .map(e -> e.asInt() != null ? e.asInt() : e.asString()).toArray();
  }

  @Override
  public boolean moveNext() {
    return this.scan.next();
  }

  @Override
  public void reset() {
    this.scan.seekToQueryStart();
  }

  @Override
  public void close() {
    this.scan.close();
  }

}