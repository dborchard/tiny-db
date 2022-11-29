package edu.utdallas.davisbase.server.a_frontend.impl.sqlite;

import edu.utdallas.davisbase.antlr.derby.SQLiteLexer;
import edu.utdallas.davisbase.antlr.derby.SQLiteParser;
import edu.utdallas.davisbase.server.a_frontend.impl.sqlite.visitors.TableNameListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public class AntlrTest {

    public static void main(String[] args) throws Exception {
        String sql = "SELECT A from T1;";
        SQLiteLexer lexer = new SQLiteLexer(CharStreams.fromString(sql));
        SQLiteParser parser = new SQLiteParser(new CommonTokenStream(lexer));

        TableNameListener tableNameListener = new TableNameListener(parser);
        parser.addParseListener(tableNameListener);
        System.out.println(parser.parse());

        System.out.println(tableNameListener.getValue());
    }

}
