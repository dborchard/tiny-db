package simpledb.b1_metadata;

import simpledb.e_record.Layout;
import simpledb.e_record.Schema;
import simpledb.b1_metadata.index.IndexInfo;
import simpledb.b1_metadata.index.IndexMgr;
import simpledb.b1_metadata.table.TableMgr;
import simpledb.f_tx.Transaction;

import java.util.Map;

public class MetadataMgr {
   private static TableMgr tblmgr;
   private static IndexMgr idxmgr;
   
   public MetadataMgr(boolean isnew, Transaction tx) {
      tblmgr  = new TableMgr(isnew, tx);
      idxmgr  = new IndexMgr(isnew, tblmgr, tx);
   }
   
   public void createTable(String tblname, Schema sch, Transaction tx) {
      tblmgr.createTable(tblname, sch, tx);
   }
   
   public Layout getLayout(String tblname, Transaction tx) {
      return tblmgr.getLayout(tblname, tx);
   }
   

   public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
      idxmgr.createIndex(idxname, tblname, fldname, tx);
   }
   
   public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
      return idxmgr.getIndexInfo(tblname, tx);
   }
}
