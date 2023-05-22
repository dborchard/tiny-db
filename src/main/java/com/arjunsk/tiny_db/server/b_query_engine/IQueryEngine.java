package com.arjunsk.tiny_db.server.b_query_engine;

import com.arjunsk.tiny_db.server.b_query_engine.common.dto.TableDto;

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
