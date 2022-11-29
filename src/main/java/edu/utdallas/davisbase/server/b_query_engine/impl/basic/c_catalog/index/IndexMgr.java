package edu.utdallas.davisbase.server.b_query_engine.impl.basic.c_catalog.index;

import edu.utdallas.davisbase.server.b_query_engine.impl.basic.b_stats_manager.StatMgr;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.c_catalog.table.TableMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.TableScan;
import edu.utdallas.davisbase.server.d_storage_engine.a_ondisk.a_file_organization.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_ondisk.a_file_organization.heap.RecordValueSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * The index manager.
 * The index manager has similar functionality to the table manager.
 *
 * @author Edward Sciore
 */
public class IndexMgr {
    private RecordValueLayout recordValueLayout;
    private TableMgr tblmgr;
    private StatMgr statmgr;

    /**
     * Create the index manager.
     * This constructor is called during system startup.
     * If the database is new, then the <i>idxcat</i> table is created.
     *
     * @param isnew indicates whether this is a new database
     * @param tx    the system startup transaction
     */
    public IndexMgr(boolean isnew, TableMgr tblmgr, StatMgr statmgr, Transaction tx) {
        if (isnew) {
            RecordValueSchema sch = new RecordValueSchema();
            sch.addStringField("indexname", TableMgr.MAX_NAME);
            sch.addStringField("tablename", TableMgr.MAX_NAME);
            sch.addStringField("fieldname", TableMgr.MAX_NAME);
            tblmgr.createTable("idxcat", sch, tx);
        }
        this.tblmgr = tblmgr;
        this.statmgr = statmgr;
        recordValueLayout = tblmgr.getLayout("idxcat", tx);
    }

    /**
     * Create an index of the specified type for the specified field.
     * A unique ID is assigned to this index, and its information
     * is stored in the idxcat table.
     *
     * @param idxname the name of the index
     * @param tblname the name of the indexed table
     * @param fldname the name of the indexed field
     * @param tx      the calling transaction
     */
    public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
        TableScan ts = new TableScan(tx, "idxcat", recordValueLayout);
        ts.seekToHead_Insert();
        ts.setString("indexname", idxname);
        ts.setString("tablename", tblname);
        ts.setString("fieldname", fldname);
        ts.close();
    }

    /**
     * Return a map containing the index info for all indexes
     * on the specified table.
     *
     * @param tblname the name of the table
     * @param tx      the calling transaction
     * @return a map of IndexInfo objects, keyed by their field names
     */
    public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
        Map<String, IndexInfo> result = new HashMap<String, IndexInfo>();
        TableScan ts = new TableScan(tx, "idxcat", recordValueLayout);
        while (ts.next()) if (ts.getString("tablename").equals(tblname)) {
            String idxname = ts.getString("indexname");
            String fldname = ts.getString("fieldname");
            RecordValueLayout tblRecordValueLayout = tblmgr.getLayout(tblname, tx);
            StatInfo tblsi = statmgr.getStatInfo(tblname, tblRecordValueLayout, tx);
            IndexInfo ii = new IndexInfo(idxname, fldname, tblRecordValueLayout.schema(), tx, tblsi);
            result.put(fldname, ii);
        }
        ts.close();
        return result;
    }
}
