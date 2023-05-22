package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer;

import com.arjunsk.tiny_db.server.a_frontend.IParser;
import com.arjunsk.tiny_db.server.a_frontend.common.domain.commands.*;
import com.arjunsk.tiny_db.server.a_frontend.impl.mysql.MySqlParser;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.QueryPlanner;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.UpdatePlanner;
import com.arjunsk.tiny_db.server.d_storage_engine.common.transaction.Transaction;


/**
 * The composite planner handling Query and Update.
 *
 * @author Edward Sciore
 */
public class BasicPlanner {
    private final QueryPlanner queryPlanner;
    private final UpdatePlanner updatePlanner;

    public BasicPlanner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan createQueryPlan(String qry, Transaction tx) {
        IParser parser = new MySqlParser(qry);
        QueryData data = parser.queryCmd();
//        System.out.println(data);
        return queryPlanner.createPlan(data, tx);
    }

    public int executeUpdate(String cmd, Transaction tx) {
        IParser parser = new MySqlParser(cmd);
        Object data = parser.updateCmd();
//        System.out.println(data);
        if (data instanceof InsertData) return updatePlanner.executeInsert((InsertData) data, tx);
        else if (data instanceof DeleteData) return updatePlanner.executeDelete((DeleteData) data, tx);
        else if (data instanceof ModifyData) return updatePlanner.executeModify((ModifyData) data, tx);
        else if (data instanceof CreateTableData) return updatePlanner.executeCreateTable((CreateTableData) data, tx);
        else if (data instanceof CreateIndexData) return updatePlanner.executeCreateIndex((CreateIndexData) data, tx);
        else return 0;
    }

}
