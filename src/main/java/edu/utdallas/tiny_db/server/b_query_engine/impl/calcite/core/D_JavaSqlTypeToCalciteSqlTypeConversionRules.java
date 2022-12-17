package edu.utdallas.tiny_db.server.b_query_engine.impl.calcite.core;

import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Mapping for Our Storage Engine Datatype and Calcite Data Type
 *
 * @author Arjun Sunil Kumar
 */
public class D_JavaSqlTypeToCalciteSqlTypeConversionRules {

  private static final D_JavaSqlTypeToCalciteSqlTypeConversionRules INSTANCE = new D_JavaSqlTypeToCalciteSqlTypeConversionRules();

  private final Map<Integer, SqlTypeName> rules = ImmutableMap.<Integer, SqlTypeName>builder()
      .put(Types.INTEGER, SqlTypeName.INTEGER).put(Types.VARCHAR, SqlTypeName.VARCHAR).build();

  public static D_JavaSqlTypeToCalciteSqlTypeConversionRules instance() {
    return INSTANCE;
  }


  public SqlTypeName lookup(Integer name) {
    return rules.getOrDefault(name, SqlTypeName.ANY);
  }
}