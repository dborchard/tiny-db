package com.arjunsk.tiny_db.server.b_query_engine.impl.calcite.core;

import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Calcite Schema containing multiple table.
 * <p>
 * This schema will be injected as Proxy to the JDBMs schema in connection.
 *
 * @author Arjun Sunil Kumar
 */
public class C_Schema extends AbstractSchema {

  private final Map<String, Table> tableMap;

  public C_Schema(Map<String, Table> tableMap) {
    this.tableMap = tableMap;
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }

}