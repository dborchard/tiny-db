package com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.a_naive;

import com.arjunsk.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import com.arjunsk.tiny_db.server.a_frontend.common.domain.commands.*;
import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.MetadataMgr;
import com.arjunsk.tiny_db.server.d_storage_engine.RWRecordScan;
import com.arjunsk.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.B_SelectPlan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.A_TablePlan;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.UpdatePlanner;

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
        Plan p = new A_TablePlan(tx, data.tableName(), mdm);
        RWRecordScan scan = (RWRecordScan) p.open();
        scan.seekToInsertStart();
        Iterator<D_Constant> iter = data.vals().iterator();
        for (String fldname : data.fields()) {
            D_Constant val = iter.next();
            scan.setVal(fldname, val);
        }
        scan.close();
        return 1;
    }


    public int executeModify(ModifyData data, Transaction tx) {
        Plan p = new A_TablePlan(tx, data.tableName(), mdm);
        p = new B_SelectPlan(p, data.pred());
        RWRecordScan scan = (RWRecordScan) p.open();
        int count = 0;
        while (scan.next()) {
            D_Constant val = data.newValue().evaluate(scan);
            scan.setVal(data.targetField(), val);
            count++;
        }
        scan.close();
        return count;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        Plan p = new A_TablePlan(tx, data.tableName(), mdm);
        p = new B_SelectPlan(p, data.pred());
        RWRecordScan us = (RWRecordScan) p.open();
        int count = 0;
        while (us.next()) {
            us.delete();
            count++;
        }
        us.close();
        return count;
    }


}
