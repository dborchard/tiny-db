package edu.utdallas.davisbase.server.b_query_engine.impl.basic;

import edu.utdallas.davisbase.server.b_query_engine.IQueryEngine;
import edu.utdallas.davisbase.server.b_query_engine.common.TableDto;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.Planner;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.UpdatePlanner;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.b_rule_base.BetterQueryPlanner;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.b_rule_base.BetterUpdatePlanner;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.c_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.common.scans.Scan;
import edu.utdallas.davisbase.server.d_storage_engine.common.file.FileMgr;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BasicQueryEngine implements IQueryEngine {
    public static int BLOCK_SIZE = 512;

    private final FileMgr fm;
    private final Planner planner;

    public BasicQueryEngine(String dirname) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, BLOCK_SIZE);
        Transaction tx = newTx();

        MetadataMgr mdm = new MetadataMgr(fm.isNew(), tx);
        QueryPlanner qp = new BetterQueryPlanner(mdm);
        UpdatePlanner up = new BetterUpdatePlanner(mdm);
        planner = new Planner(qp, up);

        tx.commit();
    }


    public TableDto doQuery(String sql) {
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

        return new TableDto(columnNames, rows);
    }


    public TableDto doUpdate(String sql) {
        Transaction tx = newTx();
        int updatedRows = planner.executeUpdate(sql, tx);
        tx.commit();

        String message = updatedRows + " " + (updatedRows == 1 ? "row" : "rows") + " updated.";
        return new TableDto(message);
    }

    public void close() {
        System.out.println("Shutting down");
    }

    private Transaction newTx() {
        return new Transaction(fm);
    }

    public FileMgr fileMgr() {
        return fm;
    }
}


