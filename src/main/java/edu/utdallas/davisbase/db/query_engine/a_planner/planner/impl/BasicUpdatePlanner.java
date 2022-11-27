package edu.utdallas.davisbase.db.query_engine.a_planner.planner.impl;

import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;
import edu.utdallas.davisbase.db.frontend.domain.commands.*;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.Plan;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.SelectPlan;
import edu.utdallas.davisbase.db.query_engine.a_planner.plan.impl.TablePlan;
import edu.utdallas.davisbase.db.query_engine.a_planner.planner.UpdatePlanner;
import edu.utdallas.davisbase.db.query_engine.b_metadata.MetadataMgr;
import edu.utdallas.davisbase.db.storage_engine.a_scans.UpdateScan;
import edu.utdallas.davisbase.db.storage_engine.Transaction;

import java.util.Iterator;

/**
 * The basic planner for SQL update statements.
 *
 * @author sciore
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
        UpdateScan us = (UpdateScan) p.open();
        us.seekToHead_Update();
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
        UpdateScan us = (UpdateScan) p.open();
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
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.next()) {
            us.delete();
            count++;
        }
        us.close();
        return count;
    }


}
