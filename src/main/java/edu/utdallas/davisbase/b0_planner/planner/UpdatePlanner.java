package edu.utdallas.davisbase.b0_planner.planner;

import edu.utdallas.davisbase.c_parse.domain.commands.*;
import edu.utdallas.davisbase.f_tx.Transaction;


public interface UpdatePlanner {

    int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);

    int executeInsert(InsertData data, Transaction tx);

    int executeModify(ModifyData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

}
