package edu.utdallas.davisbase.server.b_query_engine.b_stats_manager;

import edu.utdallas.davisbase.server.b_query_engine.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.table.TableMgr;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.regular.TableScan;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueLayout;

import java.util.HashMap;
import java.util.Map;

public class StatMgr {
    private int numcalls;
    private Map<String, StatInfo> tablestats;
    private TableMgr tblMgr;

    public StatMgr(TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tblname, RecordValueLayout layout, Transaction tx) {
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
        numcalls = 0;
        RecordValueLayout tcatlayout = tblMgr.getLayout("tblcat", tx);
        TableScan tcat = new TableScan(tx, "tblcat", tcatlayout);
        while (tcat.next()) {
            String tblname = tcat.getString("tblname");
            RecordValueLayout layout = tblMgr.getLayout(tblname, tx);
            StatInfo si = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, si);
        }
        tcat.close();
    }

    private synchronized StatInfo calcTableStats(String tblname, RecordValueLayout layout, Transaction tx) {
        int numRecs = 0;
        int numblocks = 0;
        TableScan ts = new TableScan(tx, tblname, layout);
        while (ts.next()) {
            numRecs++;
            numblocks = ts.getRid().blockNumber() + 1;
        }
        ts.close();
        return new StatInfo(numblocks, numRecs);
    }
}
