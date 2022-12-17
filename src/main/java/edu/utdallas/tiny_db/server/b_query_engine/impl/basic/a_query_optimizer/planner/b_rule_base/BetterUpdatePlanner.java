package edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.b_rule_base;


import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.commands.*;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.Plan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.B_SelectPlan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.plan.impl.A_TablePlan;
import edu.utdallas.tiny_db.server.b_query_engine.impl.basic.a_query_optimizer.planner.UpdatePlanner;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.index.IndexInfo;
import edu.utdallas.tiny_db.server.d_storage_engine.RWRecordScan;
import edu.utdallas.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import edu.utdallas.tiny_db.server.d_storage_engine.RWIndexScan;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.page.RecordKey;

import java.util.Iterator;
import java.util.Map;


/**
 * The Update Planner with Indexes.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
public class BetterUpdatePlanner implements UpdatePlanner {
    private MetadataMgr mdm;

    public BetterUpdatePlanner(MetadataMgr mdm) {
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
        String tblname = data.tableName();
        Plan p = new A_TablePlan(tx, tblname, mdm);

        // first, insert the record
        RWRecordScan s = (RWRecordScan) p.open();
        s.seekToInsertStart();

        // then modify each field, inserting an index record if appropriate
        RecordKey recordKey = s.getRid();
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
        Iterator<D_Constant> valIter = data.vals().iterator();
        for (String fldname : data.fields()) {
            D_Constant val = valIter.next();
            s.setVal(fldname, val);

            IndexInfo ii = indexes.get(fldname);
            if (ii != null) {
                RWIndexScan idx = ii.open();
                idx.insert(val, recordKey);
                idx.close();
            }
        }
        s.close();
        return 1;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        String tblname = data.tableName();
        Plan p = new A_TablePlan(tx, tblname, mdm);
        p = new B_SelectPlan(p, data.pred());
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);

        RWRecordScan s = (RWRecordScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, delete the record's RID from every index
            RecordKey recordKey = s.getRid();
            for (String fldname : indexes.keySet()) {
                D_Constant val = s.getVal(fldname);
                RWIndexScan idx = indexes.get(fldname).open();
                idx.delete(val, recordKey);
                idx.close();
            }
            // then delete the record
            s.delete();
            count++;
        }
        s.close();
        return count;
    }

    public int executeModify(ModifyData data, Transaction tx) {
        String tblname = data.tableName();
        String fldname = data.targetField();
        Plan p = new A_TablePlan(tx, tblname, mdm);
        p = new B_SelectPlan(p, data.pred());

        IndexInfo ii = mdm.getIndexInfo(tblname, tx).get(fldname);
        RWIndexScan idx = (ii == null) ? null : ii.open();

        RWRecordScan s = (RWRecordScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, update the record
            D_Constant newval = data.newValue().evaluate(s);
            D_Constant oldval = s.getVal(fldname);
            s.setVal(data.targetField(), newval);

            // then update the appropriate index, if it exists
            if (idx != null) {
                RecordKey recordKey = s.getRid();
                idx.delete(oldval, recordKey);
                idx.insert(newval, recordKey);
            }
            count++;
        }
        if (idx != null) idx.close();
        s.close();
        return count;
    }


}
