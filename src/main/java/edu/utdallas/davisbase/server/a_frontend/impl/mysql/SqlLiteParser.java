package edu.utdallas.davisbase.server.a_frontend.impl.mysql;

import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.*;
import edu.utdallas.davisbase.server.a_frontend.IParser;
import edu.utdallas.davisbase.server.a_frontend.impl.mysql.visitors.QueryStatementVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;
import org.apache.shardingsphere.sql.parser.mysql.parser.MySQLLexer;

public class SqlLiteParser implements IParser {

    QueryStatementVisitor queryStatementVisitor;

    MySQLStatementParser parser;

    public SqlLiteParser(String sql) {

        MySQLLexer lexer = new MySQLLexer(CharStreams.fromString(sql));
        MySQLStatementParser parser = new MySQLStatementParser(new CommonTokenStream(lexer));

        queryStatementVisitor = new QueryStatementVisitor(parser);
    }

    @Override
    public QueryData queryCmd() {
        queryStatementVisitor.visit(parser.execute());
        return queryStatementVisitor.getValue();
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
