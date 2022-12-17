package edu.utdallas.tiny_db.server.d_storage_engine.impl.index.bplustree.advanced.serde;

import com.github.davidmoten.bplustree.LargeByteBuffer;
import com.github.davidmoten.bplustree.Serializer;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;

public class ConstantSerializer implements Serializer<D_Constant> {

  @Override
  public D_Constant read(LargeByteBuffer bb) {
    int ival = bb.getInt();
    return new D_Constant(ival);
  }

  @Override
  public void write(LargeByteBuffer bb, D_Constant c) {
    bb.putInt(c.asInt());
  }

  @Override
  public int maxSize() {

    return Integer.BYTES;
  }


}
