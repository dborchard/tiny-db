package edu.utdallas.davisbase.b0_planner;

import edu.utdallas.davisbase.c_parse.Parser;
import edu.utdallas.davisbase.c_parse.domain.commands.*;
import edu.utdallas.davisbase.f_tx.Transaction;
import edu.utdallas.davisbase.b0_planner.plan.Plan;
import edu.utdallas.davisbase.b0_planner.planner.QueryPlanner;
import edu.utdallas.davisbase.b0_planner.planner.UpdatePlanner;


public class Planner {
    private final QueryPlanner queryPlanner;
    private final UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan createQueryPlan(String qry, Transaction tx) {
        Parser parser = new Parser(qry);
        QueryData data = parser.queryCmd();
        return queryPlanner.createPlan(data, tx);
    }

    public int executeUpdate(String cmd, Transaction tx) {
        Parser parser = new Parser(cmd);
        Object data = parser.updateCmd();
        if (data instanceof InsertData) return updatePlanner.executeInsert((InsertData) data, tx);
        else if (data instanceof DeleteData) return updatePlanner.executeDelete((DeleteData) data, tx);
        else if (data instanceof ModifyData) return updatePlanner.executeModify((ModifyData) data, tx);
        else if (data instanceof CreateTableData) return updatePlanner.executeCreateTable((CreateTableData) data, tx);
        else if (data instanceof CreateIndexData) return updatePlanner.executeCreateIndex((CreateIndexData) data, tx);
        else return 0;
    }

}
