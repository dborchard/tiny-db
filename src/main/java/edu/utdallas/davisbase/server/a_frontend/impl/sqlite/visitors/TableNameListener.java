package edu.utdallas.davisbase.server.a_frontend.impl.sqlite.visitors;


import edu.utdallas.davisbase.antlr.derby.SQLiteParser;
import edu.utdallas.davisbase.antlr.derby.SQLiteParserBaseListener;
import org.antlr.v4.runtime.Parser;


public class TableNameListener extends SQLiteParserBaseListener {

    private final Parser parser;
    private String tableName;

    public TableNameListener(Parser parser) {
        this.parser = parser;
    }


    @Override
    public void enterTable_name(SQLiteParser.Table_nameContext ctx) {
        ctx.toStringTree(parser);
        this.tableName = ctx.any_name().getText();
        super.enterTable_name(ctx);
    }


    public String getValue() {
        return tableName;
    }

}
