package edu.utdallas.davisbase.server.a_frontend.impl.sqlite;

import edu.utdallas.davisbase.antlr.derby.SQLiteLexer;
import edu.utdallas.davisbase.antlr.derby.SQLiteParser;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.B_Term;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.C_Expression;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;
import edu.utdallas.davisbase.server.a_frontend.impl.IParser;
import edu.utdallas.davisbase.server.a_frontend.impl.sqlite.visitors.TableNameListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class SqlLiteParser implements IParser {

    TableNameListener tableNameListener;

    public SqlLiteParser(String sql) {

        SQLiteLexer lexer = new SQLiteLexer(CharStreams.fromString(sql));
        SQLiteParser parser = new SQLiteParser(new CommonTokenStream(lexer));

        tableNameListener = new TableNameListener(parser);

        parser.addParseListener(tableNameListener);
    }

    @Override
    public String field() {
        return tableNameListener.getValue();
    }

    @Override
    public D_Constant constant() {
        return null;
    }

    @Override
    public C_Expression expression() {
        return null;
    }

    @Override
    public B_Term term() {
        return null;
    }

    @Override
    public A_Predicate predicate() {
        return null;
    }

    @Override
    public QueryData queryCmd() {
        return null;
    }

    @Override
    public Object updateCmd() {
        return null;
    }

    @Override
    public DeleteData delete() {
        return null;
    }

    @Override
    public InsertData insert() {
        return null;
    }

    @Override
    public ModifyData modify() {
        return null;
    }

    @Override
    public CreateTableData createTable() {
        return null;
    }

    @Override
    public CreateIndexData createIndex() {
        return null;
    }
}
