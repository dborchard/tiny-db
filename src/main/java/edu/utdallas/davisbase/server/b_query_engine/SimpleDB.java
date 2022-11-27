package edu.utdallas.davisbase.server.b_query_engine;

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
import edu.utdallas.davisbase.server.d_storage_engine.c_common.b_file.FileMgr;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SimpleDB {
    public static int BLOCK_SIZE = 512;
    public static String LOG_FILE = "davisdb.log";
    public static int BUFFER_SIZE = 8;

    private FileMgr fm;
    private BufferMgr bm;
    private LogMgr lm;
    private Planner planner;
    private MetadataMgr mdm;

    public SimpleDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, blocksize);
        lm = new LogMgr(fm, LOG_FILE);
        bm = new BufferMgr(fm, lm, buffsize);
    }

    public SimpleDB(String dirname) {
        this(dirname, BLOCK_SIZE, BUFFER_SIZE);

        Transaction tx = newTx();
        boolean isnew = fm.isNew();
        if (isnew) System.out.println("creating new database");
        else {
            System.out.println("recovering existing database");
            tx.recover();
        }
        mdm = new MetadataMgr(isnew, tx);

        QueryPlanner qp = new BetterQueryPlanner(mdm);
        UpdatePlanner up = new BetterUpdatePlanner(mdm);
        planner = new Planner(qp, up);

        tx.commit();
    }


    public Table doQuery(String sql) {
        Transaction tx = newTx();
        Plan p = planner.createQueryPlan(sql, tx);
        Scan s = p.open();

        List<String> columnNames = p.schema().fields();

        List<List<String>> rows = new ArrayList<>();
        while (s.next()) {
            List<String> row = new ArrayList<>();
            for (String field : columnNames) row.add(s.getVal(field).toString());
            rows.add(row);
        }

        s.close();
        tx.commit();

        return new Table(columnNames, rows);
    }


    public Table doUpdate(String sql) {
        Transaction tx = newTx();
        int updatedRows = planner.executeUpdate(sql, tx);
        tx.commit();

        String message = updatedRows + " " + (updatedRows == 1 ? "row" : "rows") + " updated.";
        return new Table(message);
    }

    public void close() {
        System.out.println("Shutting down");
    }

    public Transaction newTx() {
        return new Transaction(fm, lm, bm);
    }


    //Debugging Helper
    public MetadataMgr mdMgr() {
        return mdm;
    }

    public Planner planner() {
        return planner;
    }

    public FileMgr fileMgr() {
        return fm;
    }

    public LogMgr logMgr() {
        return lm;
    }

    public BufferMgr bufferMgr() {
        return bm;
    }
}


