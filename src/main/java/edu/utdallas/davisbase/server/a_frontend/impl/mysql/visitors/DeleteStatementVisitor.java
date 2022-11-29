package edu.utdallas.davisbase.server.a_frontend.impl.mysql.visitors;


import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.QueryData;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementBaseVisitor;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;

import java.util.ArrayList;
import java.util.List;


public class DeleteStatementVisitor extends MySQLStatementBaseVisitor {
    private final MySQLStatementParser parser;
    private List<String> fields;
    private String table;
    private A_Predicate pred;

    public DeleteStatementVisitor(MySQLStatementParser parser) {
        this.parser = parser;

        this.table = "";
        this.fields = new ArrayList<>();
    }

    @Override
    public Object visitTableName(MySQLStatementParser.TableNameContext ctx) {
        this.table = ctx.name().getText();
        return super.visitTableName(ctx);
    }

    @Override
    public Object visitSelectFieldsInto(MySQLStatementParser.SelectFieldsIntoContext ctx) {
        System.out.println("Point 11" + ctx.toStringTree(parser));
        return super.visitSelectFieldsInto(ctx);
    }

    @Override
    public Object visitShowWhereClause(MySQLStatementParser.ShowWhereClauseContext ctx) {
        System.out.println("Point 12" + ctx.toStringTree(parser));
        return super.visitShowWhereClause(ctx);
    }


    @Override
    public Object visitProjection(MySQLStatementParser.ProjectionContext ctx) {
        System.out.println("Point 13" + ctx.toStringTree(parser));
        this.fields.add(ctx.expr().getText());
        return super.visitProjection(ctx);
    }

    @Override
    public Object visitSimpleExpr(MySQLStatementParser.SimpleExprContext ctx) {
        System.out.println("Point 14" + ctx.toStringTree(parser));
        return super.visitSimpleExpr(ctx);
    }

    @Override
    public Object visitPredicate(MySQLStatementParser.PredicateContext ctx) {
        System.out.println("Point 15" + ctx.toStringTree(parser));
        return super.visitPredicate(ctx);
    }

    @Override
    public Object visitQueryExpression(MySQLStatementParser.QueryExpressionContext ctx) {
        System.out.println("Point 14" + ctx.toStringTree(parser));
        return super.visitQueryExpression(ctx);
    }

    @Override
    public Object visitSelect(MySQLStatementParser.SelectContext ctx) {
        System.out.println("Point 1" + ctx.queryExpression().queryExpressionBody().queryPrimary().querySpecification().projections().projection(0).toStringTree(parser));
        System.out.println(fields);
        return super.visitSelect(ctx);
    }

    @Override
    public Object visitFields(MySQLStatementParser.FieldsContext ctx) {
        System.out.println("Point 2" + ctx.toStringTree(parser));
        return super.visitFields(ctx);
    }

    @Override
    public Object visitColumnRefList(MySQLStatementParser.ColumnRefListContext ctx) {
        System.out.println("Point 3" + ctx.toStringTree(parser));
        return super.visitColumnRefList(ctx);
    }

    @Override
    public Object visitColumnNames(MySQLStatementParser.ColumnNamesContext ctx) {
        System.out.println("Point 4" + ctx.toStringTree(parser));
        return super.visitColumnNames(ctx);
    }

    @Override
    public Object visitColumnAttribute(MySQLStatementParser.ColumnAttributeContext ctx) {
        System.out.println("Point 5" + ctx.toStringTree(parser));
        return super.visitColumnAttribute(ctx);
    }

    @Override
    public Object visitColumnDefinition(MySQLStatementParser.ColumnDefinitionContext ctx) {
        System.out.println("Point 6" + ctx.toStringTree(parser));
        return super.visitColumnDefinition(ctx);
    }

    @Override
    public Object visitColumnName(MySQLStatementParser.ColumnNameContext ctx) {
        System.out.println("Point 7" + ctx.toStringTree(parser));
        return super.visitColumnName(ctx);
    }

    @Override
    public Object visitColumnRef(MySQLStatementParser.ColumnRefContext ctx) {
        System.out.println("Point 8" + ctx.toStringTree(parser));
        return super.visitColumnRef(ctx);
    }

    @Override
    public Object visitColumnFormat(MySQLStatementParser.ColumnFormatContext ctx) {
        System.out.println("Point 9" + ctx.toStringTree(parser));
        return super.visitColumnFormat(ctx);
    }

    @Override
    public Object visitShowColumns(MySQLStatementParser.ShowColumnsContext ctx) {
        System.out.println("Point 10" + ctx.toStringTree(parser));
        return super.visitShowColumns(ctx);
    }

    public QueryData getValue() {
        return new QueryData(fields, table, null);
    }

}
