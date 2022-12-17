package edu.utdallas.tiny_db.server.b_query_engine.impl.basic.b_stats_manager;

import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.b_stats_manager.domain.StatInfo;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TableMgr;
import edu.utdallas.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TablePhysicalLayout;

import java.util.HashMap;
import java.util.Map;

/**
 * State Manager for cost based query planner.
 *
 * @author Edward Sciore
 */
public class StatMgr {
    private int numcalls;
    private Map<String, StatInfo> tablestats;
    private TableMgr tblMgr;

    public StatMgr(TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tblname, TablePhysicalLayout layout, Transaction tx) {
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
        tablestats = new HashMap<String, StatInfo>();
    }

    private synchronized StatInfo calcTableStats(String tblname, TablePhysicalLayout layout, Transaction tx) {
        return null;
    }
}
