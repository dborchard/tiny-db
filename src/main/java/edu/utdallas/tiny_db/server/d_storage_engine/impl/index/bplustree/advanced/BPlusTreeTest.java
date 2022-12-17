package edu.utdallas.tiny_db.server.d_storage_engine.impl.index.bplustree.advanced;

import com.github.davidmoten.bplustree.BPlusTree;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.page.RecordKey;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.index.bplustree.advanced.serde.ConstantSerializer;
import edu.utdallas.tiny_db.server.d_storage_engine.impl.index.bplustree.advanced.serde.RecordKeySerializer;

public class BPlusTreeTest {

  public static void main(String[] args) throws Exception {
    BPlusTree<D_Constant, RecordKey> tree = BPlusTree.file().directory("tinydb")
        .maxLeafKeys(32)
        .maxNonLeafKeys(8)
        .segmentSizeMB(1)
        .keySerializer(new ConstantSerializer())
        .valueSerializer(new RecordKeySerializer())
        .naturalOrder();

    tree.insert(new D_Constant(1), new RecordKey(1, 2));
    tree.insert(new D_Constant(2), new RecordKey(3, 4));

    tree.find(new D_Constant(1)).forEach(e -> System.out.println(e));

    tree.close();
  }
}
