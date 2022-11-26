package edu.utdallas.davisbase.query_engine.a_server;

import edu.utdallas.davisbase.query_engine.b0_planner.Planner;
import edu.utdallas.davisbase.query_engine.b0_planner.plan.Plan;
import edu.utdallas.davisbase.query_engine.b0_planner.planner.QueryPlanner;
import edu.utdallas.davisbase.query_engine.b0_planner.planner.UpdatePlanner;
import edu.utdallas.davisbase.query_engine.b0_planner.planner.impl.BetterQueryPlanner;
import edu.utdallas.davisbase.query_engine.b0_planner.planner.impl.BetterUpdatePlanner;
import edu.utdallas.davisbase.query_engine.b1_metadata.MetadataMgr;
import edu.utdallas.davisbase.query_engine.d_scans.Scan;
import edu.utdallas.davisbase.storage_engine.f_tx.Transaction;
import edu.utdallas.davisbase.storage_engine.g_file.FileMgr;
import edu.utdallas.davisbase.query_engine.h_utils.AsciiTable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SimpleDB {
    public static int BLOCK_SIZE = 512;

    private final FileMgr fm;
    private Planner planner;

    public SimpleDB(String dirname, int blockSize) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, blockSize);
    }

    public SimpleDB(String dirname) {
        this(dirname, BLOCK_SIZE);
        Transaction tx = new Transaction(fm);
        MetadataMgr mdm = new MetadataMgr(fm.isNew(), tx);

        // QueryPlanner qp = new BasicQueryPlanner(mdm);
        // UpdatePlanner up = new BasicUpdatePlanner(mdm);
        QueryPlanner qp = new BetterQueryPlanner(mdm);
        UpdatePlanner up = new BetterUpdatePlanner(mdm);
        planner = new Planner(qp, up);
        tx.commit();
    }


    public void doQuery(String sql, Transaction tx) {
        Plan p = planner.createQueryPlan(sql, tx);
        Scan s = p.open();

        List<String> columnNames = p.schema().fields();

        List<List<String>> rows = new ArrayList<>();
        while (s.next()) {
            List<String> row = new ArrayList<>();
            for (String field : columnNames) row.add(s.getVal(field).toString());
            rows.add(row);
        }

        new AsciiTable().print(columnNames, rows);
        s.close();
    }


    public void doUpdate(String sql, Transaction tx) {
        int updatedRows = planner.executeUpdate(sql, tx);
        String message = updatedRows + " " + (updatedRows == 1 ? "row" : "rows") + " updated.";
        System.out.println(message);
    }

    public void close() {
        System.out.println("Shutting down");
    }

    public Transaction newTx() {
        return new Transaction(fm);
    }

    public FileMgr fileMgr() {
        return fm;
    }
}


