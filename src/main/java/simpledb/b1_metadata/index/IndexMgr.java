package simpledb.b1_metadata.index;

import simpledb.d_scans.impl.TableScan;
import simpledb.e_record.Layout;
import simpledb.e_record.Schema;
import simpledb.b1_metadata.table.TableMgr;
import simpledb.f_tx.Transaction;

import java.util.HashMap;
import java.util.Map;

import static simpledb.b1_metadata.table.TableMgr.MAX_NAME;

/**
 * The index manager.
 * The index manager has similar functionality to the table manager.
 *
 * @author Edward Sciore
 */
public class IndexMgr {
    private Layout layout;
    private TableMgr tblmgr;

    /**
     * Create the index manager.
     * This constructor is called during system startup.
     * If the database is new, then the <i>idxcat</i> table is created.
     *
     * @param isnew indicates whether this is a new database
     * @param tx    the system startup transaction
     */
    public IndexMgr(boolean isnew, TableMgr tblmgr, Transaction tx) {
        if (isnew) {
            Schema sch = new Schema();
            sch.addStringField("indexname", MAX_NAME);
            sch.addStringField("tablename", MAX_NAME);
            sch.addStringField("fieldname", MAX_NAME);
            tblmgr.createTable("idxcat", sch, tx);
        }
        this.tblmgr = tblmgr;
        layout = tblmgr.getLayout("idxcat", tx);
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
        TableScan ts = new TableScan(tx, "idxcat", layout);
        ts.insert();
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
        TableScan ts = new TableScan(tx, "idxcat", layout);
        while (ts.next()) if (ts.getString("tablename").equals(tblname)) {
            String idxname = ts.getString("indexname");
            String fldname = ts.getString("fieldname");
            Layout tblLayout = tblmgr.getLayout(tblname, tx);
            IndexInfo ii = new IndexInfo(idxname, fldname, tblLayout.schema(), tx);
            result.put(fldname, ii);
        }
        ts.close();
        return result;
    }
}
