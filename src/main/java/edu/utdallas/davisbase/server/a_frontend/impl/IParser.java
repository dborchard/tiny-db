package edu.utdallas.davisbase.server.a_frontend.impl;

import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;

public interface IParser {


    public QueryData queryCmd();

    public Object updateCmd();

    public DeleteData delete();

    public InsertData insert();

    public ModifyData modify();

    public CreateTableData createTable();

    public CreateIndexData createIndex();

}
