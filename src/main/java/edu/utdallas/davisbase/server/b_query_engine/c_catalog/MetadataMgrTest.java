package edu.utdallas.davisbase.server.b_query_engine.c_catalog;

import edu.utdallas.davisbase.server.b_query_engine.SimpleDB;
import edu.utdallas.davisbase.server.b_query_engine.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.b_query_engine.c_catalog.index.IndexInfo;
import edu.utdallas.davisbase.server.b_query_engine.d_sql_scans.TableScan;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.RecordValueSchema;

import java.util.Map;

import static java.sql.Types.INTEGER;

public class MetadataMgrTest {
    public static void main(String[] args) throws Exception {
        SimpleDB db = new SimpleDB("metadatamgrtest", 400, 8);
        Transaction tx = db.newTx();
        MetadataMgr mdm = new MetadataMgr(true, tx);

        RecordValueSchema sch = new RecordValueSchema();
        sch.addIntField("A");
        sch.addStringField("B", 9);

        // Part 1: Table Metadata
        mdm.createTable("MyTable", sch, tx);
        RecordValueLayout layout = mdm.getLayout("MyTable", tx);
        int size = layout.slotSize();
        RecordValueSchema sch2 = layout.schema();
        System.out.println("MyTable has slot size " + size);
        System.out.println("Its fields are:");
        for (String fldname : sch2.fields()) {
            String type;
            if (sch2.type(fldname) == INTEGER)
                type = "int";
            else {
                int strlen = sch2.length(fldname);
                type = "varchar(" + strlen + ")";
            }
            System.out.println(fldname + ": " + type);
        }

        // Part 2: Statistics Metadata
        TableScan ts = new TableScan(tx, "MyTable", layout);
        for (int i = 0; i < 50; i++) {
            ts.seekToHead_Insert();
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec" + n);
        }
        StatInfo si = mdm.getStatInfo("MyTable", layout, tx);
        System.out.println("B(MyTable) = " + si.blocksAccessed());
        System.out.println("R(MyTable) = " + si.recordsOutput());
        System.out.println("V(MyTable,A) = " + si.distinctValues("A"));
        System.out.println("V(MyTable,B) = " + si.distinctValues("B"));


        // Part 4: Index Metadata
        mdm.createIndex("indexA", "MyTable", "A", tx);
        mdm.createIndex("indexB", "MyTable", "B", tx);
        Map<String, IndexInfo> idxmap = mdm.getIndexInfo("MyTable", tx);

        IndexInfo ii = idxmap.get("A");
        System.out.println("B(indexA) = " + ii.blocksAccessed());
        System.out.println("R(indexA) = " + ii.recordsOutput());
        System.out.println("V(indexA,A) = " + ii.distinctValues("A"));
        System.out.println("V(indexA,B) = " + ii.distinctValues("B"));

        ii = idxmap.get("B");
        System.out.println("B(indexB) = " + ii.blocksAccessed());
        System.out.println("R(indexB) = " + ii.recordsOutput());
        System.out.println("V(indexB,A) = " + ii.distinctValues("A"));
        System.out.println("V(indexB,B) = " + ii.distinctValues("B"));
        tx.commit();
    }
}

