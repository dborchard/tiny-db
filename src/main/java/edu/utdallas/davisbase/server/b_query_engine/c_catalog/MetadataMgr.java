package edu.utdallas.davisbase.server.b_query_engine.c_catalog;

import edu.utdallas.davisbase.server.b_query_engine.b_stats_manager.StatMgr;
import edu.utdallas.davisbase.server.b_query_engine.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.index.IndexInfo;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.index.IndexMgr;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.table.TableMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;

import java.util.Map;

public class MetadataMgr {
    private static TableMgr tblmgr;
    private static IndexMgr idxmgr;
    private static StatMgr statmgr;

    public MetadataMgr(boolean isnew, Transaction tx) {
        tblmgr = new TableMgr(isnew, tx);
        statmgr = new StatMgr(tblmgr, tx);
        idxmgr = new IndexMgr(isnew, tblmgr, statmgr, tx);
    }

    public void createTable(String tblname, RecordValueSchema sch, Transaction tx) {
        tblmgr.createTable(tblname, sch, tx);
    }

    public RecordValueLayout getLayout(String tblname, Transaction tx) {
        return tblmgr.getLayout(tblname, tx);
    }


    public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
        idxmgr.createIndex(idxname, tblname, fldname, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        return idxmgr.getIndexInfo(tblname, tx);
    }

    public StatInfo getStatInfo(String tblname, RecordValueLayout layout, Transaction tx) {
        return statmgr.getStatInfo(tblname, layout, tx);
    }
}
//DONE