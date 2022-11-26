package edu.utdallas.davisbase.query_engine.b0_planner.planner;

import edu.utdallas.davisbase.storage_engine.f_tx.Transaction;
import edu.utdallas.davisbase.query_engine.c_parse.domain.commands.*;


public interface UpdatePlanner {

    int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);

    int executeInsert(InsertData data, Transaction tx);

    int executeModify(ModifyData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

}
