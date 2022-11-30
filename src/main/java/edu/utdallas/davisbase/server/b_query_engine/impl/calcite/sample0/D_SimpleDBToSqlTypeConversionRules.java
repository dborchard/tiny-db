package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.sample0;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Types;
import java.util.Map;

public class D_SimpleDBToSqlTypeConversionRules {

    private static final D_SimpleDBToSqlTypeConversionRules INSTANCE = new D_SimpleDBToSqlTypeConversionRules();

    private final Map<Integer, SqlTypeName> rules = ImmutableMap.<Integer, SqlTypeName>builder().put(Types.INTEGER, SqlTypeName.INTEGER).put(Types.VARCHAR, SqlTypeName.VARCHAR).build();

    public static D_SimpleDBToSqlTypeConversionRules instance() {
        return INSTANCE;
    }


    public SqlTypeName lookup(Integer name) {
        return rules.getOrDefault(name, SqlTypeName.ANY);
    }
}