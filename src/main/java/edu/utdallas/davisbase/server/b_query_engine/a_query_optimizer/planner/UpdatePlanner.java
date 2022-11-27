package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.planner;

import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;


public interface UpdatePlanner {

    int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);

    int executeInsert(InsertData data, Transaction tx);

    int executeModify(ModifyData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

}
