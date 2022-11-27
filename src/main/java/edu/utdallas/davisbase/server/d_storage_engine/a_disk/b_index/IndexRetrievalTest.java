package edu.utdallas.davisbase.server.d_storage_engine.a_disk.b_index;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.b_query_engine.SimpleDB;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.Plan;
import edu.utdallas.davisbase.server.b_query_engine.a_query_optimizer.plan.regular.TablePlan;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.MetadataMgr;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.index.IndexInfo;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordKey;
import edu.utdallas.davisbase.server.d_storage_engine.b_common.a_scans.UpdateScan;

import java.util.Map;

public class IndexRetrievalTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("studentdb");
        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mdMgr();

        // Open a scan on the data table.
        Plan studentplan = new TablePlan(tx, "student", mdm);
        UpdateScan studentscan = (UpdateScan) studentplan.open();

        // Open the index on MajorId.
        Map<String, IndexInfo> indexes = mdm.getIndexInfo("student", tx);
        IndexInfo ii = indexes.get("majorid");
        Index idx = ii.open();

        // Retrieve all index records having a dataval of 20.
        idx.seek(new D_Constant(20));
        while (idx.next()) {
            // Use the datarid to go to the corresponding STUDENT record.
            RecordKey datarid = idx.getRecordId();
            studentscan.moveToRid(datarid);
            System.out.println(studentscan.getString("sname"));
        }

        // Close the index and the data table.
        idx.close();
        studentscan.close();
        tx.commit();
    }
}
