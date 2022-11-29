package edu.utdallas.davisbase.server.a_frontend.impl.derby;

import edu.utdallas.davisbase.antlr.mysql.DerbyLexer;
import edu.utdallas.davisbase.antlr.mysql.DerbyParser;
import edu.utdallas.davisbase.server.a_frontend.impl.derby.visitors.TableNameListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;

public class AntlrTest {

    public static void main(String[] args) throws Exception {
        String sql = "SELECT A from T1;";
        Lexer lexer = new DerbyLexer(CharStreams.fromString(sql));
        Parser parser = new DerbyParser(new CommonTokenStream(lexer));

        TableNameListener tableName = new TableNameListener();
        parser.addParseListener(tableName);
        parser.getBuildParseTree();

        System.out.println(tableName.getValue());

    }

}
