package edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.a_naive;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.SelectPlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.TablePlan;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.a_query_optimizer.planner.UpdatePlanner;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.RWDataScan;

import java.util.Iterator;

/**
 * The Update Planner without Indexes and cost considerations.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class BasicUpdatePlanner implements UpdatePlanner {
    private MetadataMgr mdm;

    public BasicUpdatePlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    public int executeCreateTable(CreateTableData data, Transaction tx) {
        mdm.createTable(data.tableName(), data.newSchema(), tx);
        return 0;
    }

    public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
        return 0;
    }

    public int executeInsert(InsertData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        RWDataScan us = (RWDataScan) p.open();
        us.seekToHead_Insert();
        Iterator<D_Constant> iter = data.vals().iterator();
        for (String fldname : data.fields()) {
            D_Constant val = iter.next();
            us.setVal(fldname, val);
        }
        us.close();
        return 1;
    }


    public int executeModify(ModifyData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        p = new SelectPlan(p, data.pred());
        RWDataScan us = (RWDataScan) p.open();
        int count = 0;
        while (us.next()) {
            D_Constant val = data.newValue().evaluate(us);
            us.setVal(data.targetField(), val);
            count++;
        }
        us.close();
        return count;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        p = new SelectPlan(p, data.pred());
        RWDataScan us = (RWDataScan) p.open();
        int count = 0;
        while (us.next()) {
            us.delete();
            count++;
        }
        us.close();
        return count;
    }


}
