package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.core;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Types;
import java.util.Map;

public class D_JavaSqlTypeToCalciteSqlTypeConversionRules {

    private static final D_JavaSqlTypeToCalciteSqlTypeConversionRules INSTANCE = new D_JavaSqlTypeToCalciteSqlTypeConversionRules();

    private final Map<Integer, SqlTypeName> rules = ImmutableMap.<Integer, SqlTypeName>builder().put(Types.INTEGER, SqlTypeName.INTEGER).put(Types.VARCHAR, SqlTypeName.VARCHAR).build();

    public static D_JavaSqlTypeToCalciteSqlTypeConversionRules instance() {
        return INSTANCE;
    }


    public SqlTypeName lookup(Integer name) {
        return rules.getOrDefault(name, SqlTypeName.ANY);
    }
}