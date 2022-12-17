package edu.utdallas.tiny_db.server.a_frontend.impl.mysql;

import edu.utdallas.tiny_db.server.a_frontend.IParser;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.commands.QueryData;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;
import org.apache.shardingsphere.sql.parser.mysql.parser.MySQLLexer;

/**
 * The ANTLR parser for SQLite Dialect.
 *
 * @author Arjun Sunil Kumar
 */
public class SqlLiteParser implements IParser {

    SQLStatementVisitor sqlStatementVisitor;

    public SqlLiteParser(String sql) {
        MySQLLexer lexer = new MySQLLexer(CharStreams.fromString(sql));
        MySQLStatementParser parser = new MySQLStatementParser(new CommonTokenStream(lexer));

        sqlStatementVisitor = new SQLStatementVisitor(parser);
        sqlStatementVisitor.visit(parser.execute());
    }

    @Override
    public QueryData queryCmd() {
        return (QueryData) sqlStatementVisitor.getValue();
    }

    @Override
    public Object updateCmd() {
        return sqlStatementVisitor.getValue();
    }

}
