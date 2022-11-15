package edu.utdallas.davisbase.b0_planner.planner;

import edu.utdallas.davisbase.c_parse.commands.*;
import edu.utdallas.davisbase.f_tx.Transaction;
import simpledb.c_parse.commands.*;


public interface UpdatePlanner {
    int executeInsert(InsertData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

    int executeModify(ModifyData data, Transaction tx);

    int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);
}
