package edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.planner.b_rule_base;


import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.impl.SelectPlan;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.impl.TablePlan;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.planner.UpdatePlanner;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.index.IndexInfo;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.c_common.a_scans.UpdateScan;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.b_index.Index;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordKey;

import java.util.Iterator;
import java.util.Map;

/**
 * A modification of the basic update planner.
 * It dispatches each update statement to the corresponding
 * index planner.
 *
 * @author Edward Sciore
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
        Plan p = new TablePlan(tx, tblname, mdm);

        // first, insert the record
        UpdateScan s = (UpdateScan) p.open();
        s.seekToHead_Insert();

        // then modify each field, inserting an index record if appropriate
        RecordKey recordKey = s.getRid();
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);
        Iterator<D_Constant> valIter = data.vals().iterator();
        for (String fldname : data.fields()) {
            D_Constant val = valIter.next();
            s.setVal(fldname, val);

            IndexInfo ii = indexes.get(fldname);
            if (ii != null) {
                Index idx = ii.open();
                idx.insert(val, recordKey);
                idx.close();
            }
        }
        s.close();
        return 1;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        String tblname = data.tableName();
        Plan p = new TablePlan(tx, tblname, mdm);
        p = new SelectPlan(p, data.pred());
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tblname, tx);

        UpdateScan s = (UpdateScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, delete the record's RID from every index
            RecordKey recordKey = s.getRid();
            for (String fldname : indexes.keySet()) {
                D_Constant val = s.getVal(fldname);
                Index idx = indexes.get(fldname).open();
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
        Plan p = new TablePlan(tx, tblname, mdm);
        p = new SelectPlan(p, data.pred());

        IndexInfo ii = mdm.getIndexInfo(tblname, tx).get(fldname);
        Index idx = (ii == null) ? null : ii.open();

        UpdateScan s = (UpdateScan) p.open();
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
