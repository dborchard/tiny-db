package edu.utdallas.davisbase.b2_index.btree;

import edu.utdallas.davisbase.e_record.Layout;
import edu.utdallas.davisbase.e_record.RID;
import edu.utdallas.davisbase.e_record.Schema;
import edu.utdallas.davisbase.f_tx.Transaction;
import edu.utdallas.davisbase.c_parse.domain.clause.D_Constant;
import edu.utdallas.davisbase.g_file.BlockId;
import edu.utdallas.davisbase.b2_index.Index;

import static java.sql.Types.INTEGER;

/**
 * A B-tree implementation of the Index interface.
 *
 * @author Edward Sciore
 */
public class BTreeIndex implements Index {
    private Transaction tx;
    private Layout dirLayout, leafLayout;
    private String leaftbl;
    private BTreeLeaf leaf = null;
    private BlockId rootblk;


    public BTreeIndex(Transaction tx, String idxname, Layout leafLayout) {
        this.tx = tx;
        // deal with the leaves
        leaftbl = idxname + "leaf";
        this.leafLayout = leafLayout;
        if (tx.size(leaftbl) == 0) {
            BlockId blk = tx.append(leaftbl);
            BTPage node = new BTPage(tx, blk, leafLayout);
            node.format(blk, -1);
        }

        // deal with the directory
        Schema dirsch = new Schema();
        dirsch.add("block", leafLayout.schema());
        dirsch.add("dataval", leafLayout.schema());
        String dirtbl = idxname + "dir";
        dirLayout = new Layout(dirsch);
        rootblk = new BlockId(dirtbl, 0);
        if (tx.size(dirtbl) == 0) {
            // create new root block
            tx.append(dirtbl);
            BTPage node = new BTPage(tx, rootblk, dirLayout);
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
        BTreeDir root = new BTreeDir(tx, rootblk, dirLayout);
        int blknum = root.search(searchkey);
        root.close();
        BlockId leafblk = new BlockId(leaftbl, blknum);
        leaf = new BTreeLeaf(tx, leafblk, leafLayout, searchkey);
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
     * @see Index#getDataRid()
     */
    public RID getDataRid() {
        return leaf.getDataRid();
    }


    public void insert(D_Constant dataval, RID datarid) {
        seek(dataval);
        DirEntry e = leaf.insert(datarid);
        leaf.close();
        if (e == null) return;
        BTreeDir root = new BTreeDir(tx, rootblk, dirLayout);
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
     * @see Index#delete(simpledb.d_scans.Constant, RID)
     */
    public void delete(D_Constant dataval, RID datarid) {
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
