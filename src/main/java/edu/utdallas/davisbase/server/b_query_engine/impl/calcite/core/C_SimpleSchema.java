package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.core;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class C_SimpleSchema extends AbstractSchema {

    private final Map<String, Table> tableMap;

    public C_SimpleSchema(Map<String, Table> tableMap) {
        this.tableMap = tableMap;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

}