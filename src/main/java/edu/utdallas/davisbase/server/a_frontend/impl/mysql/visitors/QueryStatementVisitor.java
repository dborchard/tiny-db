package edu.utdallas.davisbase.server.a_frontend.impl.mysql.visitors;


import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.B_Term;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.C_Expression;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.QueryData;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementBaseVisitor;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;

import java.util.ArrayList;
import java.util.List;


public class QueryStatementVisitor extends MySQLStatementBaseVisitor {
    private final MySQLStatementParser parser;
    private final List<String> fields;
    private String table;
    private A_Predicate pred;

    public QueryStatementVisitor(MySQLStatementParser parser) {
        this.parser = parser;

        this.table = "";
        this.fields = new ArrayList<>();
        this.pred = new A_Predicate();

    }

    @Override
    public Object visitTableName(MySQLStatementParser.TableNameContext ctx) {
        this.table = ctx.name().getText();
        return super.visitTableName(ctx);
    }


    @Override
    public Object visitProjection(MySQLStatementParser.ProjectionContext ctx) {
        this.fields.add(ctx.expr().getText());
        return super.visitProjection(ctx);
    }


    @Override
    public Object visitExpr(MySQLStatementParser.ExprContext ctx) {
        if (ctx.booleanPrimary() != null && ctx.booleanPrimary().comparisonOperator() != null && ctx.booleanPrimary().comparisonOperator() != null && ctx.booleanPrimary().comparisonOperator().getText().equals("=")) {
            B_Term term = getTerm(ctx.booleanPrimary());
            pred.conjoinWith(new A_Predicate(term));
        }
        return super.visitExpr(ctx);
    }

    private B_Term getTerm(MySQLStatementParser.BooleanPrimaryContext term) {
        MySQLStatementParser.BooleanPrimaryContext lhs = term.booleanPrimary();
        MySQLStatementParser.PredicateContext rhs = term.predicate();

        C_Expression lhsExp = new C_Expression(lhs.getText());
        C_Expression rhsExp = new C_Expression(rhs.getText());
        return new B_Term(lhsExp, rhsExp);
    }


    public QueryData getValue() {
        return new QueryData(fields, table, pred);
    }

}
