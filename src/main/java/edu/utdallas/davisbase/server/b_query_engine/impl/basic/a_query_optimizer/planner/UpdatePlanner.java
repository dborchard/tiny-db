package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner;

import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;
import edu.utdallas.davisbase.server.d_storage_engine.common.transaction.Transaction;


/**
 * The Update Planner
 *
 * @author Edward Sciore
 */
public interface UpdatePlanner {

    int executeCreateTable(CreateTableData data, Transaction tx);

    int executeCreateIndex(CreateIndexData data, Transaction tx);

    int executeInsert(InsertData data, Transaction tx);

    int executeModify(ModifyData data, Transaction tx);

    int executeDelete(DeleteData data, Transaction tx);

}
