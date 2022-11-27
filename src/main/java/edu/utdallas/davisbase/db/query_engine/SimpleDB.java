package edu.utdallas.davisbase.db.query_engine;

import edu.utdallas.davisbase.db.query_engine.a_planner.planner.impl.BetterUpdatePlanner;
import edu.utdallas.davisbase.db.query_engine.b_metadata.MetadataMgr;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.Transaction;
import edu.utdallas.davisbase.db.storage_engine.b_file.FileMgr;
import edu.utdallas.davisbase.db.query_engine.a_planner.Planner;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.query_engine.a_planner.planner.QueryPlanner;
import edu.utdallas.davisbase.db.query_engine.a_planner.planner.UpdatePlanner;
import edu.utdallas.davisbase.db.query_engine.a_planner.planner.impl.BetterQueryPlanner;
import edu.utdallas.davisbase.db.query_engine.d_scans.Scan;
import edu.utdallas.davisbase.cli.TablePrinter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SimpleDB {
    public static int BLOCK_SIZE = 512;

    private final FileMgr fm;
    private final Planner planner;

    public SimpleDB(String dirname) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, BLOCK_SIZE);
        Transaction tx = newTx();

        MetadataMgr mdm = new MetadataMgr(fm.isNew(), tx);
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

        new TablePrinter().print(columnNames, rows);
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


