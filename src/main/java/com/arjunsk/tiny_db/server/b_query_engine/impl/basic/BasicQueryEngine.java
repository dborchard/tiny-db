package com.arjunsk.tiny_db.server.b_query_engine.impl.basic;

import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.MetadataMgr;
import com.arjunsk.tiny_db.server.b_query_engine.common.dto.TableDto;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.BasicPlanner;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.UpdatePlanner;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.b_rule_base.BetterQueryPlanner;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.b_rule_base.BetterUpdatePlanner;
import com.arjunsk.tiny_db.server.d_storage_engine.RORecordScan;
import com.arjunsk.tiny_db.server.d_storage_engine.common.file.FileMgr;
import com.arjunsk.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import com.arjunsk.tiny_db.server.b_query_engine.IQueryEngine;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
/**
 * The ANTLR parser for SQLite Dialect.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class BasicQueryEngine implements IQueryEngine {
    public static int BLOCK_SIZE = 512;

    private final FileMgr fm;
    private final BasicPlanner planner;

    public BasicQueryEngine(String dirname) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, BLOCK_SIZE);
        Transaction tx = newTx();

        MetadataMgr mdm = new MetadataMgr(fm.isNew(), tx);
        QueryPlanner qp = new BetterQueryPlanner(mdm);
        UpdatePlanner up = new BetterUpdatePlanner(mdm);
        planner = new BasicPlanner(qp, up);

        tx.commit();
    }


    public TableDto doQuery(String sql) {
        Transaction tx = newTx();
        Plan p = planner.createQueryPlan(sql, tx);
        RORecordScan s = p.open();

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


