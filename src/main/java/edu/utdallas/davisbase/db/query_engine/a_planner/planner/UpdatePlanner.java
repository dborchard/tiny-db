package edu.utdallas.davisbase.db.query_engine.a_planner.planner;

import edu.utdallas.davisbase.db.frontend.domain.commands.*;
import edu.utdallas.davisbase.db.storage_engine.b_transaction.Transaction;


public interface UpdatePlanner {

    int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);

    int executeInsert(InsertData data, Transaction tx);

    int executeModify(ModifyData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

}
