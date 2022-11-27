package edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr;

import edu.utdallas.davisbase.server.b_query_engine.SimpleDB;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.Planner;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.planner.QueryPlanner;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.planner.UpdatePlanner;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.planner.b_rule_base.BetterQueryPlanner;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.planner.b_rule_base.BetterUpdatePlanner;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.b_query_engine.e_dto.Table;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.LogMgr;
import edu.utdallas.davisbase.server.d_storage_engine.b_buffer_mgr.BufferMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.BlockId;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.FileMgr;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.Page;

import java.io.IOException;

public class BufferFileTest {
   public static void main(String[] args) throws IOException {
      SimpleDB db = new SimpleDB("bufferfiletest", 400, 8);
      BufferMgr bm = db.bufferMgr();
      BlockId blk = new BlockId("testfile", 2);
      int pos1 = 88;
      
      Buffer b1 = bm.pin(blk);
      Page p1 = b1.contents();
      p1.setString(pos1, "abcdefghijklm");
      int size = Page.maxLength("abcdefghijklm".length());
      int pos2 = pos1 + size;
      p1.setInt(pos2, 345);
      b1.setModified(1, 0);
      bm.unpin(b1);
      
      Buffer b2 = bm.pin(blk);
      Page p2 = b2.contents();
      System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
      System.out.println("offset " + pos1 + " contains " + p2.getString(pos1));
      bm.unpin(b2);
   }
}