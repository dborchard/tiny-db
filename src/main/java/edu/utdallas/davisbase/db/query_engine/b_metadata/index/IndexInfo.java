package edu.utdallas.davisbase.db.query_engine.b_metadata.index;

import edu.utdallas.davisbase.db.query_engine.e_record.Layout;
import edu.utdallas.davisbase.db.query_engine.e_record.Schema;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.Index;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.btree.BTreeIndex;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.Transaction;

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
    private Schema tblSchema;
    private Layout idxLayout;


    public IndexInfo(String idxname, String fldname, Schema tblSchema, Transaction tx) {
        this.idxname = idxname;
        this.fldname = fldname;
        this.tx = tx;
        this.tblSchema = tblSchema;
        this.idxLayout = createIdxLayout();
    }


    public Index open() {
        return new BTreeIndex(tx, idxname, idxLayout);
    }


    private Layout createIdxLayout() {
        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        if (tblSchema.type(fldname) == INTEGER) sch.addIntField("dataval");
        else {
            int fldlen = tblSchema.length(fldname);
            sch.addStringField("dataval", fldlen);
        }
        return new Layout(sch);
    }
}
