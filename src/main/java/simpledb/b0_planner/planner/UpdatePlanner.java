package simpledb.b0_planner.planner;

import simpledb.c_parse.commands.*;
import simpledb.f_tx.Transaction;


public interface UpdatePlanner {
    int executeInsert(InsertData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

    int executeModify(ModifyData data, Transaction tx);

    int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);
}
