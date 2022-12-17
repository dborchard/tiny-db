package edu.utdallas.tiny_db.server.d_storage_engine.impl.data.heap.page;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * An identifier for a record within a file. A RID consists of the block number in the file, and the
 * location of the record in that block.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
public class RecordKey implements Serializable {

  private static final long serialVersionUID = 1L;


  private int blockNumber;
  private int slotNumber;
}
