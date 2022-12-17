package edu.utdallas.tiny_db.server.a_frontend.impl.mysql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;
import org.apache.shardingsphere.sql.parser.mysql.parser.MySQLLexer;

/**
 * Test to verify ANTLR is working fine.
 *
 * @author Arjun Sunil Kumar
 */
public class AntlrParserTest {

    public static void main(String[] args) {
        outputCommand("SELECT A,B,C from T1 where A=1;");
//        outputCommand("DELETE from T1 where A=10;");
//        outputCommand("CREATE INDEX A_IDX on T2(A);");
//        outputCommand("insert into T2 (A, B) values (1, 'Alice');");
//        outputCommand("update T1 SET A=1 where A=2;");
//        outputCommand("create table T2 ( A int, B varchar(9) );");
    }

    private static void outputCommand(String sql) {
        MySQLLexer lexer = new MySQLLexer(CharStreams.fromString(sql));
        MySQLStatementParser parser = new MySQLStatementParser(new CommonTokenStream(lexer));
        MySQLStatementParser.ExecuteContext execute = parser.execute();

        SQLStatementVisitor sqlStatementVisitor = new SQLStatementVisitor(parser);
        System.out.println(execute.toStringTree(parser));
        sqlStatementVisitor.visit(execute);

        System.out.println(sqlStatementVisitor.getValue().toString());
    }

}
