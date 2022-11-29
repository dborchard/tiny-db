package edu.utdallas.davisbase.server.a_frontend.impl.mysql;


import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.B_Term;
import edu.utdallas.davisbase.server.a_frontend.common.domain.clause.C_Expression;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.CreateIndexData;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.DeleteData;
import edu.utdallas.davisbase.server.a_frontend.common.domain.commands.QueryData;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementBaseVisitor;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;

import java.util.ArrayList;
import java.util.List;


public class SQLStatementVisitor extends MySQLStatementBaseVisitor {
    private final MySQLStatementParser parser;
    private COMMAND_TYPE commandType;

    // Common
    private String tableName;
    private A_Predicate predicate;

    // Select
    private final List<String> selectFields;

    // Index
    private String indexName;
    private String indexFieldName;

    public SQLStatementVisitor(MySQLStatementParser parser) {
        this.parser = parser;

        this.tableName = "";
        this.predicate = new A_Predicate();

        this.selectFields = new ArrayList<>();

        this.indexName = "";
        this.indexFieldName = "";
    }

    // COMMAND TYPE
    @Override
    public Object visitCreateIndex(MySQLStatementParser.CreateIndexContext ctx) {
        commandType = COMMAND_TYPE.CREATE_INDEX;
        return super.visitCreateIndex(ctx);
    }

    @Override
    public Object visitCreateTable(MySQLStatementParser.CreateTableContext ctx) {
        commandType = COMMAND_TYPE.CREATE_TABLE;
        return super.visitCreateTable(ctx);
    }


    @Override
    public Object visitDelete(MySQLStatementParser.DeleteContext ctx) {
        commandType = COMMAND_TYPE.DELETE;
        return super.visitDelete(ctx);
    }

    @Override
    public Object visitInsert(MySQLStatementParser.InsertContext ctx) {
        commandType = COMMAND_TYPE.INSERT;
        return super.visitInsert(ctx);
    }

    @Override
    public Object visitUpdate(MySQLStatementParser.UpdateContext ctx) {
        commandType = COMMAND_TYPE.MODIFY;
        return super.visitUpdate(ctx);
    }

    @Override
    public Object visitSelect(MySQLStatementParser.SelectContext ctx) {
        commandType = COMMAND_TYPE.QUERY;
        return super.visitSelect(ctx);
    }

    // Command Attributes for Query & Delete

    @Override
    public Object visitTableName(MySQLStatementParser.TableNameContext ctx) {
        this.tableName = ctx.name().getText();
        return super.visitTableName(ctx);
    }


    @Override
    public Object visitProjection(MySQLStatementParser.ProjectionContext ctx) {
        this.selectFields.add(ctx.expr().getText());
        return super.visitProjection(ctx);
    }


    @Override
    public Object visitExpr(MySQLStatementParser.ExprContext ctx) {
        if (ctx.booleanPrimary() != null && ctx.booleanPrimary().comparisonOperator() != null && ctx.booleanPrimary().comparisonOperator() != null && ctx.booleanPrimary().comparisonOperator().getText().equals("=")) {
            B_Term term = getTerm(ctx.booleanPrimary());
            predicate.conjoinWith(new A_Predicate(term));
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

    // Command Attributes for CreateIndex


    @Override
    public Object visitIndexName(MySQLStatementParser.IndexNameContext ctx) {
        this.indexName = ctx.getText();
        return super.visitIndexName(ctx);
    }

    @Override
    public Object visitKeyPart(MySQLStatementParser.KeyPartContext ctx) {
        this.indexFieldName = ctx.getText();
        return super.visitKeyPart(ctx);
    }

    public Object getValue() {
        switch (commandType) {
            case QUERY:
                return new QueryData(selectFields, tableName, predicate);
            case DELETE:
                return new DeleteData(tableName, predicate);
            case CREATE_INDEX:
                return new CreateIndexData(indexName, tableName, indexFieldName);
        }
        return new QueryData(selectFields, tableName, predicate);
    }


    enum COMMAND_TYPE {
        QUERY, MODIFY, INSERT, DELETE, CREATE_TABLE, CREATE_INDEX
    }
}
