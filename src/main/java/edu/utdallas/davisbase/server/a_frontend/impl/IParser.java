package edu.utdallas.davisbase.server.a_frontend.impl;

import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.B_Term;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.C_Expression;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;

public interface IParser {

    public String field();

    public D_Constant constant();

    public C_Expression expression();

    public B_Term term();

    public A_Predicate predicate();

    public QueryData queryCmd();

    public Object updateCmd();

    public DeleteData delete();

    public InsertData insert();

    public ModifyData modify();

    public CreateTableData createTable();

    public CreateIndexData createIndex();

}
