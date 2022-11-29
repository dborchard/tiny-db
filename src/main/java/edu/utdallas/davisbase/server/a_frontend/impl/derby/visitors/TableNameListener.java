package edu.utdallas.davisbase.server.a_frontend.impl.derby.visitors;


import edu.utdallas.davisbase.antlr.mysql.DerbyParser;
import edu.utdallas.davisbase.antlr.mysql.DerbyParserBaseListener;

public class TableNameListener extends DerbyParserBaseListener {

    String tableName;

    @Override
    public void enterTable_name(DerbyParser.Table_nameContext ctx) {
        tableName = ctx.getText();
        super.enterTable_name(ctx);
    }

    public String getValue() {
        return tableName;
    }
}
