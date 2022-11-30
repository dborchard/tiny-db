package edu.utdallas.davisbase.server.b_query_engine;

import edu.utdallas.davisbase.server.b_query_engine.common.dto.TableDto;

/**
 * Query Engine interface. We can implement using Calcite or Custom.
 *
 * @author Arjun Sunil Kumar
 */
public interface IQueryEngine {

    public TableDto doQuery(String sql);

    public TableDto doUpdate(String sql);

    public void close();
}
