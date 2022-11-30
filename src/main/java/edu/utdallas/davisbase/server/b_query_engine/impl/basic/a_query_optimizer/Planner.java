package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer;

import edu.utdallas.davisbase.server.a_frontend.IParser;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;
import edu.utdallas.davisbase.server.a_frontend.impl.mysql.SqlLiteParser;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.UpdatePlanner;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;


public class Planner {
    private final QueryPlanner queryPlanner;
    private final UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan createQueryPlan(String qry, Transaction tx) {
        IParser parser = new SqlLiteParser(qry);
        QueryData data = parser.queryCmd();
        System.out.println(data);
        return queryPlanner.createPlan(data, tx);
    }

    public int executeUpdate(String cmd, Transaction tx) {
        IParser parser = new SqlLiteParser(cmd);
        Object data = parser.updateCmd();
        System.out.println(data);
        if (data instanceof InsertData) return updatePlanner.executeInsert((InsertData) data, tx);
        else if (data instanceof DeleteData) return updatePlanner.executeDelete((DeleteData) data, tx);
        else if (data instanceof ModifyData) return updatePlanner.executeModify((ModifyData) data, tx);
        else if (data instanceof CreateTableData) return updatePlanner.executeCreateTable((CreateTableData) data, tx);
        else if (data instanceof CreateIndexData) return updatePlanner.executeCreateIndex((CreateIndexData) data, tx);
        else return 0;
    }

}
