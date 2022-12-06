package edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree.advanced;

import com.github.davidmoten.bplustree.BPlusTree;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.d_storage_engine.common.transaction.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.RWIndexScan;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.heap.page.RecordKey;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.table.TablePhysicalLayout;
import edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree.advanced.serde.ConstantSerializer;
import edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree.advanced.serde.RecordKeySerializer;
import java.util.Iterator;
import lombok.SneakyThrows;


/**
 * B+ tree index using https://github.com/davidmoten/bplustree
 *
 * @author Arjun Sunil Kumar
 */
public class AdvancedBPlusTreeIndex implements RWIndexScan {

  BPlusTree<D_Constant, RecordKey> tree;
  Iterator<RecordKey> iterator;

  public AdvancedBPlusTreeIndex(Transaction tx, String idxname, TablePhysicalLayout leafRecordValueLayout) {
    tree = BPlusTree.file().directory("davisdb")
        .maxLeafKeys(32)
        .maxNonLeafKeys(8)
        .segmentSizeMB(1)
        .keySerializer(new ConstantSerializer())
        .valueSerializer(new RecordKeySerializer())
        .naturalOrder();
  }

  @Override
  public void insert(D_Constant key, RecordKey value) {
    tree.insert(key, value);
  }

  @Override
  public void delete(D_Constant key, RecordKey value) {
    throw new RuntimeException("Unimplemented by library. To support later");
  }

  @Override
  public void seek(D_Constant key) {
    iterator = tree.find(key).iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public RecordKey next() {
    return iterator.next();
  }

  @SneakyThrows
  @Override
  public void close() {
    tree.close();
  }


}
