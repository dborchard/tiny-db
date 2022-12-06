package edu.utdallas.davisbase.server.d_storage_engine.impl.index.bplustree.advanced.serde;

import com.github.davidmoten.bplustree.LargeByteBuffer;
import com.github.davidmoten.bplustree.Serializer;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.heap.page.RecordKey;

public class RecordKeySerializer implements Serializer<RecordKey> {

  @Override
  public RecordKey read(LargeByteBuffer bb) {
    int blknum = bb.getInt();
    int slot = bb.getInt();
    return new RecordKey(blknum, slot);
  }

  @Override
  public void write(LargeByteBuffer bb, RecordKey recordKey) {
    bb.putInt(recordKey.getBlockNumber());
    bb.putInt(recordKey.getSlotNumber());
  }

  @Override
  public int maxSize() {
    return Integer.BYTES + Integer.BYTES;
  }

}
