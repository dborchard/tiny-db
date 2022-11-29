package edu.utdallas.davisbase.server.b_query_engine.impl.calcite;

import edu.utdallas.davisbase.server.b_query_engine.IQueryEngine;
import edu.utdallas.davisbase.server.b_query_engine.common.TableDto;

public class CalciteQueryEngine implements IQueryEngine {
    @Override
    public TableDto doQuery(String sql) {
        return null;
    }

    @Override
    public TableDto doUpdate(String sql) {
        return null;
    }

    @Override
    public void close() {

    }
}
