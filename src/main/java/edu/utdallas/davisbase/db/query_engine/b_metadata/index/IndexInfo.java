package edu.utdallas.davisbase.db.query_engine.b_metadata.index;

import edu.utdallas.davisbase.db.storage_engine.b_io.data.heap.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.b_io.data.heap.TableSchema;
import edu.utdallas.davisbase.db.storage_engine.Transaction;
import edu.utdallas.davisbase.db.storage_engine.b_io.index.Index;
import edu.utdallas.davisbase.db.storage_engine.Index_BTree;

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


    public IndexInfo(String idxname, String fldname, TableSchema tblTableSchema, Transaction tx) {
        this.idxname = idxname;
        this.fldname = fldname;
        this.tx = tx;
        this.tblTableSchema = tblTableSchema;
        this.idxTableFileLayout = createIdxLayout();
    }


    public Index open() {
        return new Index_BTree(tx, idxname, idxTableFileLayout);
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
}
