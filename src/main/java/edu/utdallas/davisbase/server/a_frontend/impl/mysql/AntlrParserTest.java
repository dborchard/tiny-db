package edu.utdallas.davisbase.server.a_frontend.impl.mysql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;
import org.apache.shardingsphere.sql.parser.mysql.parser.MySQLLexer;

public class AntlrParserTest {

    public static void main(String[] args) {
        outputCommand("SELECT A,B,C from T1 where A=10 AND B=15 AND C=1;");
        outputCommand("DELETE from T1 where A=10;");
        outputCommand("CREATE INDEX A_IDX on T2(A);");
    }

    private static void outputCommand(String sql1) {
        MySQLLexer lexer = new MySQLLexer(CharStreams.fromString(sql1));
        MySQLStatementParser parser = new MySQLStatementParser(new CommonTokenStream(lexer));

        SQLStatementVisitor tableNameListener = new SQLStatementVisitor(parser);
        MySQLStatementParser.ExecuteContext execute = parser.execute();
        System.out.println(execute.toStringTree(parser));
        tableNameListener.visit(execute);

        System.out.println(tableNameListener.getValue().toString());
    }

}
