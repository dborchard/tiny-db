package edu.utdallas.tiny_db.server.b_query_engine.common.catalog.index;

import static java.sql.Types.INTEGER;

import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.b_stats_manager.domain.StatInfo;
import edu.utdallas.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import edu.utdallas.tiny_db.server.d_storage_engine.RWIndexScan;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TablePhysicalLayout;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.index.bplustree.basic.BasicBPlusTreeIndex;


/**
 * The information about an index. This information is used by the query planner in order to
 * estimate the costs of using the index, and to obtain the layout of the index records. Its methods
 * are essentially the same as those of Plan.
 *
 * @author Edward Sciore
 */
public class IndexInfo {

  private String idxname, fldname;
  private Transaction tx;
  private TableDefinition tblTableDefinition;
  private TablePhysicalLayout idxRecordValueLayout;
  private StatInfo si;


  public IndexInfo(String idxname, String fldname, TableDefinition tblTableDefinition,
      Transaction tx, StatInfo si) {
    this.idxname = idxname;
    this.fldname = fldname;
    this.tx = tx;
    this.tblTableDefinition = tblTableDefinition;
    this.idxRecordValueLayout = createIdxLayout();
    this.si = si;
  }


  public RWIndexScan open() {
    return new BasicBPlusTreeIndex(tx, idxname, idxRecordValueLayout);
  }


  private TablePhysicalLayout createIdxLayout() {
    // Schema contains Block, Id, DataValue
    TableDefinition sch = new TableDefinition();
    sch.addIntField("block");
    sch.addIntField("id");
    if (tblTableDefinition.type(fldname) == INTEGER) {
      sch.addIntField("dataval");
    } else {
      int fldlen = tblTableDefinition.length(fldname);
      sch.addStringField("dataval", fldlen);
    }
    return new TablePhysicalLayout(sch);
  }

  public int blocksAccessed() {
    int rpb = tx.blockSize() / idxRecordValueLayout.slotSize();
    int numblocks = si.recordsOutput() / rpb;
    return BasicBPlusTreeIndex.searchCost(numblocks, rpb);
  }

  public int recordsOutput() {
    return si.recordsOutput() / si.distinctValues(fldname);
  }
}
