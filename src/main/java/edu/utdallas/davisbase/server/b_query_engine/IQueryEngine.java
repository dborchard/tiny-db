package edu.utdallas.davisbase.server.b_query_engine;

import edu.utdallas.davisbase.server.b_query_engine.common.TableDto;

public interface IQueryEngine {

    public TableDto doQuery(String sql);

    public TableDto doUpdate(String sql) ;

    public void close();
}
