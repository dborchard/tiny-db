package edu.utdallas.davisbase.db.query_engine.b_metadata;

import edu.utdallas.davisbase.db.query_engine.b_metadata.index.IndexInfo;
import edu.utdallas.davisbase.db.query_engine.b_metadata.index.IndexMgr;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableSchema;
import edu.utdallas.davisbase.db.storage_engine.b_transaction.Transaction;
import edu.utdallas.davisbase.db.query_engine.b_metadata.table.TableMgr;

import java.util.Map;

public class MetadataMgr {
   private static TableMgr tblmgr;
   private static IndexMgr idxmgr;
   
   public MetadataMgr(boolean isnew, Transaction tx) {
      tblmgr  = new TableMgr(isnew, tx);
      idxmgr  = new IndexMgr(isnew, tblmgr, tx);
   }
   
   public void createTable(String tblname, TableSchema sch, Transaction tx) {
      tblmgr.createTable(tblname, sch, tx);
   }
   
   public TableFileLayout getLayout(String tblname, Transaction tx) {
      return tblmgr.getLayout(tblname, tx);
   }
   

   public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
      idxmgr.createIndex(idxname, tblname, fldname, tx);
   }
   
   public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
      return idxmgr.getIndexInfo(tblname, tx);
   }
}
