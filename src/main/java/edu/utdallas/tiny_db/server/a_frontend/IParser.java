package edu.utdallas.tiny_db.server.a_frontend;

import edu.utdallas.tiny_db.server.a_frontend.common.domain.commands.QueryData;

/**
 * Parser interface to parsing SQL statements to QueryEngine Command Objects.
 *
 * @author Arjun Sunil Kumar
 */
public interface IParser {

    public QueryData queryCmd();

    public Object updateCmd();
}
