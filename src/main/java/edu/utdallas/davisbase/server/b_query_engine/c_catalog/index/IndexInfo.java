package edu.utdallas.davisbase.server.b_query_engine.c_catalog.index;

import edu.utdallas.davisbase.server.b_query_engine.b_stats_manager.domain.StatInfo;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.BTreeIndex;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.b_index.Index;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.TableFileLayout;
import edu.utdallas.davisbase.server.d_storage_engine.a_disk.a_file_organization.heap.TableSchema;

import static java.sql.Types.INTEGER;


/**
 * The information about an index.
 * This information is used by the query planner in order to
 * estimate the costs of using the index,
 * and to obtain the layout of the index records.
 * Its methods are essentially the same as those of Plan.
 *
 * @author Edward Sciore
 */
public class IndexInfo {
    private String idxname, fldname;
    private Transaction tx;
    private TableSchema tblTableSchema;
    private TableFileLayout idxTableFileLayout;
    private StatInfo si;


    public IndexInfo(String idxname, String fldname, TableSchema tblTableSchema, Transaction tx, StatInfo si) {
        this.idxname = idxname;
        this.fldname = fldname;
        this.tx = tx;
        this.tblTableSchema = tblTableSchema;
        this.idxTableFileLayout = createIdxLayout();
        this.si = si;
    }


    public Index open() {
        return new BTreeIndex(tx, idxname, idxTableFileLayout);
    }


    private TableFileLayout createIdxLayout() {
        // Schema contains Block, Id, DataValue
        TableSchema sch = new TableSchema();
        sch.addIntField("block");
        sch.addIntField("id");
        if (tblTableSchema.type(fldname) == INTEGER) sch.addIntField("dataval");
        else {
            int fldlen = tblTableSchema.length(fldname);
            sch.addStringField("dataval", fldlen);
        }
        return new TableFileLayout(sch);
    }

    public int blocksAccessed() {
        int rpb = tx.blockSize() / idxTableFileLayout.slotSize();
        int numblocks = si.recordsOutput() / rpb;
        return BTreeIndex.searchCost(numblocks, rpb);
    }

    public int recordsOutput() {
        return si.recordsOutput() / si.distinctValues(fldname);
    }

    public int distinctValues(String fname) {
        return fldname.equals(fname) ? 1 : si.distinctValues(fldname);
    }
}
