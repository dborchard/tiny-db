package edu.utdallas.davisbase.db.storage_engine;

import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableFileLayout;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.RecordId;
import edu.utdallas.davisbase.db.storage_engine.a_io.data.heap.TableSchema;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.Index;
import edu.utdallas.davisbase.db.frontend.domain.clause.D_Constant;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.btree.BTreeDir;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.btree.BTreeLeaf;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.btree.common.BTPage;
import edu.utdallas.davisbase.db.storage_engine.a_io.index.btree.common.DirEntry;
import edu.utdallas.davisbase.db.storage_engine.b_transaction.Transaction;
import edu.utdallas.davisbase.db.storage_engine.d_file.BlockId;

import static java.sql.Types.INTEGER;

/**
 * A B-tree implementation of the Index interface.
 *
 * @author Edward Sciore
 */
public class Index_BTree implements Index {
    private Transaction tx;
    private TableFileLayout dirTableFileLayout, leafTableFileLayout;
    private String leaftbl;
    private BTreeLeaf leaf = null;
    private BlockId rootblk;


    public Index_BTree(Transaction tx, String idxname, TableFileLayout leafTableFileLayout) {
        this.tx = tx;
        // deal with the leaves
        leaftbl = idxname + "leaf";
        this.leafTableFileLayout = leafTableFileLayout;
        if (tx.size(leaftbl) == 0) {
            BlockId blk = tx.append(leaftbl);
            BTPage node = new BTPage(tx, blk, leafTableFileLayout);
            node.format(blk, -1);
        }

        // deal with the directory
        TableSchema dirsch = new TableSchema();
        dirsch.add("block", leafTableFileLayout.schema());
        dirsch.add("dataval", leafTableFileLayout.schema());
        String dirtbl = idxname + "dir";
        dirTableFileLayout = new TableFileLayout(dirsch);
        rootblk = new BlockId(dirtbl, 0);
        if (tx.size(dirtbl) == 0) {
            // create new root block
            tx.append(dirtbl);
            BTPage node = new BTPage(tx, rootblk, dirTableFileLayout);
            node.format(rootblk, 0);
            // insert initial directory entry
            int fldtype = dirsch.type("dataval");
            D_Constant minval = (fldtype == INTEGER) ? new D_Constant(Integer.MIN_VALUE) : new D_Constant("");
            node.insertDir(0, minval, 0);
            node.close();
        }
    }


    public void seek(D_Constant searchkey) {
        close();
        BTreeDir root = new BTreeDir(tx, rootblk, dirTableFileLayout);
        int blknum = root.search(searchkey);
        root.close();
        BlockId leafblk = new BlockId(leaftbl, blknum);
        leaf = new BTreeLeaf(tx, leafblk, leafTableFileLayout, searchkey);
    }

    /**
     * Move to the next leaf record having the
     * previously-specified search key.
     * Returns false if there are no more such leaf records.
     *
     * @see Index#next()
     */
    public boolean next() {
        return leaf.next();
    }

    /**
     * Return the dataRID value from the current leaf record.
     *
     * @see Index#getRecordId()
     */
    public RecordId getRecordId() {
        return leaf.getDataRid();
    }


    public void insert(D_Constant dataval, RecordId datarid) {
        seek(dataval);
        DirEntry e = leaf.insert(datarid);
        leaf.close();
        if (e == null) return;
        BTreeDir root = new BTreeDir(tx, rootblk, dirTableFileLayout);
        DirEntry e2 = root.insert(e);
        if (e2 != null) root.makeNewRoot(e2);
        root.close();
    }

    /**
     * Delete the specified index record.
     * The method first traverses the directory to find
     * the leaf page containing that record; then it
     * deletes the record from the page.
     *
     * @see Index#delete(simpledb.d_scans.Constant, RecordId)
     */
    public void delete(D_Constant dataval, RecordId datarid) {
        seek(dataval);
        leaf.delete(datarid);
        leaf.close();
    }

    /**
     * Close the index by closing its open leaf page,
     * if necessary.
     *
     * @see Index#close()
     */
    public void close() {
        if (leaf != null) leaf.close();
    }

    /**
     * Estimate the number of block accesses
     * required to find all index records having
     * a particular search key.
     *
     * @param numblocks the number of blocks in the B-tree directory
     * @param rpb       the number of index entries per block
     * @return the estimated traversal cost
     */
    public static int searchCost(int numblocks, int rpb) {
        return 1 + (int) (Math.log(numblocks) / Math.log(rpb));
    }
}
