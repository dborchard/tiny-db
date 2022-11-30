package edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree;

import com.github.davidmoten.bplustree.BPlusTree;
import com.github.davidmoten.bplustree.Serializer;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.RWIndexScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordKey;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordValueLayout;
import edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree.utils.ByteUtils;

import java.util.Iterator;

public class BPlusTreeIndex implements RWIndexScan {

    BPlusTree<byte[], byte[]> tree;
    Iterator<byte[]> iterator;

    public BPlusTreeIndex(Transaction tx, String idxname, RecordValueLayout leafRecordValueLayout) {
        tree = BPlusTree.file().directory("davisdb").maxLeafKeys(32).maxNonLeafKeys(8).segmentSizeMB(1).keySerializer(Serializer.bytes(20)).valueSerializer(Serializer.bytes(20)).naturalOrder();
    }

    @Override
    public void insert(D_Constant key, RecordKey value) {
        byte[] k = ByteUtils.convertToBytes(key);
        byte[] v = ByteUtils.convertToBytes(value);
        tree.insert(k, v);
    }

    @Override
    public void delete(D_Constant key, RecordKey value) {
        throw new RuntimeException("Unimplemented by library. To support later");
    }

    @Override
    public void seek(D_Constant key) {
        byte[] k = ByteUtils.convertToBytes(key);
        iterator = tree.find(k).iterator();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public RecordKey next() {
        byte[] result = iterator.next();
        return (RecordKey) ByteUtils.convertFromBytes(result);
    }

    @Override
    public void close() {
        try {
            tree.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
