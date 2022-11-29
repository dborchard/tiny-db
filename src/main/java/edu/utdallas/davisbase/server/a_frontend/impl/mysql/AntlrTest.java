package edu.utdallas.davisbase.server.a_frontend.impl.mysql;

import edu.utdallas.davisbase.server.a_frontend.impl.mysql.visitors.QueryStatementVisitor;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;
import org.apache.shardingsphere.sql.parser.mysql.parser.MySQLLexer;

public class AntlrTest {

    public static void main(String[] args) throws Exception {
        String sql = "SELECT A,B from T1 where A=10;";
        MySQLLexer lexer = new MySQLLexer(CharStreams.fromString(sql));
        MySQLStatementParser parser = new MySQLStatementParser(new CommonTokenStream(lexer));

        QueryStatementVisitor tableNameListener = new QueryStatementVisitor(parser);

        MySQLStatementParser.ExecuteContext execute = parser.execute();
        tableNameListener.visit(execute);

        System.out.println(tableNameListener.getValue().toString());
    }

}
