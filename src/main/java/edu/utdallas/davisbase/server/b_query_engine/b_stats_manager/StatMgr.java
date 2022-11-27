package edu.utdallas.davisbase.server.b_query_engine.b_stats_manager;

import edu.utdallas.davisbase.server.b_query_engine.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.table.TableMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.file_organization.heap.TableFileLayout;

import java.util.Map;

public class StatMgr {
    private int numcalls;
    private Map<String, StatInfo> tablestats;
    private TableMgr tblMgr;

    public StatMgr(TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tblname, TableFileLayout layout, Transaction tx) {
        numcalls++;
        if (numcalls > 100) refreshStatistics(tx);
        StatInfo si = tablestats.get(tblname);
        if (si == null) {
            si = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, si);
        }
        return si;
    }

    private synchronized void refreshStatistics(Transaction tx) {
    }

    private synchronized StatInfo calcTableStats(String tblname, TableFileLayout layout, Transaction tx) {
        return null;
    }
}
