package simpledb.a_server;

import simpledb.b0_planner.Planner;
import simpledb.b0_planner.plan.Plan;
import simpledb.b0_planner.planner.QueryPlanner;
import simpledb.b0_planner.planner.UpdatePlanner;
import simpledb.b0_planner.planner.impl.BetterQueryPlanner;
import simpledb.b0_planner.planner.impl.BetterUpdatePlanner;
import simpledb.b1_metadata.MetadataMgr;
import simpledb.d_scans.Scan;
import simpledb.f_tx.Transaction;
import simpledb.g_file.FileMgr;
import simpledb.h_utils.AsciiTable;

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
        boolean isNew = fm.isNew();
        MetadataMgr mdm = new MetadataMgr(isNew, tx);

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
        planner.executeUpdate(sql, tx);
    }

    public void close() {
    }

    public Transaction newTx() {
        return new Transaction(fm);
    }

    public FileMgr fileMgr() {
        return fm;
    }
}


