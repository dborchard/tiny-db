package edu.utdallas.davisbase.server.d_storage_engine.common.file;


import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The BlockId is used as an identifier to the Block in a db file.
 *
 * @author Edward Sciore, Arjun Sunil Kumar
 */
@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
public class BlockId {

  private final String fileName;
  private final int blockNumber;
}
