package com.arjunsk.tiny_db.server.d_storage_engine.impl.index.bplustree.basic;

import com.arjunsk.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import com.arjunsk.tiny_db.server.d_storage_engine.common.file.BlockId;
import com.arjunsk.tiny_db.server.d_storage_engine.impl.index.bplustree.basic.common.BTPage;
import com.arjunsk.tiny_db.server.d_storage_engine.impl.index.bplustree.basic.common.DirEntry;
import com.arjunsk.tiny_db.server.d_storage_engine.common.transaction.Transaction;
import com.arjunsk.tiny_db.server.d_storage_engine.RWIndexScan;
import com.arjunsk.tiny_db.server.d_storage_engine.impl.data.heap.page.RecordKey;
import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.table.TablePhysicalLayout;
import com.arjunsk.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;

import static java.sql.Types.INTEGER;

/**
 * A B-tree implementation of the Index interface.
 *
 * @author Edward Sciore
 */
public class BasicBPlusTreeIndex implements RWIndexScan {
    private Transaction tx;
    private TablePhysicalLayout dirRecordValueLayout, leafRecordValueLayout;
    private String leaftbl;
    private BTreeLeaf leaf = null;
    private BlockId rootblk;


    public BasicBPlusTreeIndex(Transaction tx, String idxname, TablePhysicalLayout leafRecordValueLayout) {
        this.tx = tx;
        // deal with the leaves
        leaftbl = idxname + "leaf";
        this.leafRecordValueLayout = leafRecordValueLayout;
        if (tx.blockCount(leaftbl) == 0) {
            BlockId blk = tx.append(leaftbl);
            BTPage node = new BTPage(tx, blk, leafRecordValueLayout);
            node.format(blk, -1);
        }

        // deal with the directory
        TableDefinition dirsch = new TableDefinition();
        dirsch.add("block", leafRecordValueLayout.schema());
        dirsch.add("dataval", leafRecordValueLayout.schema());
        String dirtbl = idxname + "dir";
        dirRecordValueLayout = new TablePhysicalLayout(dirsch);
        rootblk = new BlockId(dirtbl, 0);
        if (tx.blockCount(dirtbl) == 0) {
            // create new root block
            tx.append(dirtbl);
            BTPage node = new BTPage(tx, rootblk, dirRecordValueLayout);
            node.format(rootblk, 0);
            // insert initial directory entry
            int fldtype = dirsch.type("dataval");
            D_Constant minval = (fldtype == INTEGER) ? new D_Constant(Integer.MIN_VALUE) : new D_Constant("");
            node.insertDir(0, minval, 0);
            node.close();
        }
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

    public void seek(D_Constant key) {
        close();
        BTreeDir root = new BTreeDir(tx, rootblk, dirRecordValueLayout);
        int blknum = root.search(key);
        root.close();
        BlockId leafblk = new BlockId(leaftbl, blknum);
        leaf = new BTreeLeaf(tx, leafblk, leafRecordValueLayout, key);
    }

    /**
     * Move to the next leaf record having the
     * previously-specified search key.
     * Returns false if there are no more such leaf records.
     *
     * @see RWIndexScan#hasNext()
     */
    public boolean hasNext() {
        return leaf.next();
    }

    /**
     * Return the dataRID value from the current leaf record.
     *
     * @see RWIndexScan#next()
     */
    public RecordKey next() {
        return leaf.getDataRid();
    }

    public void insert(D_Constant key, RecordKey value) {
        seek(key);
        DirEntry e = leaf.insert(value);
        leaf.close();
        if (e == null) return;

        BTreeDir root = new BTreeDir(tx, rootblk, dirRecordValueLayout);
        DirEntry e2 = root.insert(e);
        if (e2 != null) root.makeNewRoot(e2);
        root.close();
    }

    /**
     * Delete the specified index record.
     * The method first traverses the directory to find
     * the leaf page containing that record; then it
     * deletes the record from the page.
     */
    public void delete(D_Constant key, RecordKey value) {
        seek(key);
        leaf.delete(value);
        leaf.close();
    }

    /**
     * Close the index by closing its open leaf page,
     * if necessary.
     *
     * @see RWIndexScan#close()
     */
    public void close() {
        if (leaf != null) leaf.close();
    }
}
