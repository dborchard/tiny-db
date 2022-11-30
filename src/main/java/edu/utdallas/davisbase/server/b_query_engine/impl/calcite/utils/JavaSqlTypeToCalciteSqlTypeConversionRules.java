package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.utils;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Types;
import java.util.Map;

public class JavaSqlTypeToCalciteSqlTypeConversionRules {

    private static final JavaSqlTypeToCalciteSqlTypeConversionRules INSTANCE = new JavaSqlTypeToCalciteSqlTypeConversionRules();

    private final Map<Integer, SqlTypeName> rules = ImmutableMap.<Integer, SqlTypeName>builder().put(Types.INTEGER, SqlTypeName.INTEGER).put(Types.VARCHAR, SqlTypeName.VARCHAR).build();

    public static JavaSqlTypeToCalciteSqlTypeConversionRules instance() {
        return INSTANCE;
    }


    public SqlTypeName lookup(Integer name) {
        return rules.getOrDefault(name, SqlTypeName.ANY);
    }
}