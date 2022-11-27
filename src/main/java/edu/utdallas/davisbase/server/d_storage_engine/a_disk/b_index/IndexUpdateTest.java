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

import java.util.HashMap;
import java.util.Map;

public class IndexUpdateTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("studentdb");
        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mdMgr();
        Plan studentplan = new TablePlan(tx, "student", mdm);
        UpdateScan studentscan = (UpdateScan) studentplan.open();

        // Create a map containing all indexes for STUDENT.
        Map<String, Index> indexes = new HashMap<>();
        Map<String, IndexInfo> idxinfo = mdm.getIndexInfo("student", tx);
        for (String fldname : idxinfo.keySet()) {
            Index idx = idxinfo.get(fldname).open();
            indexes.put(fldname, idx);
        }

        // Task 1: insert a new STUDENT record for Sam
        //    First, insert the record into STUDENT.
        studentscan.seekToHead_Insert();
        studentscan.setInt("sid", 11);
        studentscan.setString("sname", "sam");
        studentscan.setInt("gradyear", 2023);
        studentscan.setInt("majorid", 30);

        //    Then insert a record into each of the indexes.
        RecordKey datarid = studentscan.getRid();
        for (String fldname : indexes.keySet()) {
            D_Constant dataval = studentscan.getVal(fldname);
            Index idx = indexes.get(fldname);
            idx.insert(dataval, datarid);
        }

        // Task 2: find and delete Joe's record
        studentscan.seekToHead_Query();
        while (studentscan.next()) {
            if (studentscan.getString("sname").equals("joe")) {

                // First, delete the index records for Joe.
                RecordKey joeRid = studentscan.getRid();
                for (String fldname : indexes.keySet()) {
                    D_Constant dataval = studentscan.getVal(fldname);
                    Index idx = indexes.get(fldname);
                    idx.delete(dataval, joeRid);
                }

                // Then delete Joe's record in STUDENT.
                studentscan.delete();
                break;
            }
        }

        // Print the records to verify the updates.
        studentscan.seekToHead_Query();
        while (studentscan.next()) {
            System.out.println(studentscan.getString("sname") + " " + studentscan.getInt("sid"));
        }
        studentscan.close();

        for (Index idx : indexes.values())
            idx.close();
        tx.commit();
    }
}
