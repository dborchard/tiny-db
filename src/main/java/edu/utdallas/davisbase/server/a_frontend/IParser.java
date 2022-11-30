package edu.utdallas.davisbase.server.a_frontend;

import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;

public interface IParser {

    public QueryData queryCmd();

    public Object updateCmd();
}
