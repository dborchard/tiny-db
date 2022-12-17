package edu.utdallas.tiny_db.server.a_frontend.impl.mysql;


import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.A_Predicate;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.B_Term;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.C_Expression;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.clause.D_Constant;
import edu.utdallas.tiny_db.server.a_frontend.common.domain.commands.*;
import edu.utdallas.tiny_db.server.b_query_engine.common.catalog.table.TableDefinition;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementBaseVisitor;
import org.apache.shardingsphere.sql.parser.autogen.MySQLStatementParser;

import java.util.ArrayList;
import java.util.List;

/**
 * The ANTLR visitor class for Parsing SQL Statements.
 *
 * @author Arjun Sunil Kumar
 */
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

    // Insert
    private final List<String> insertFields;
    private final List<D_Constant> insertValues;

    // Modify
    private C_Expression updateFieldValue;
    private String updateFieldName;


    // Create Table
    private TableDefinition schema;

    public SQLStatementVisitor(MySQLStatementParser parser) {
        this.parser = parser;

        this.tableName = "";
        this.predicate = new A_Predicate();

        this.selectFields = new ArrayList<>();

        this.indexName = "";
        this.indexFieldName = "";

        this.insertFields = new ArrayList<>();
        this.insertValues = new ArrayList<>();

        this.updateFieldName = "";

        this.schema = new TableDefinition();
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
        C_Expression rhsExp = null;

        if (rhs.bitExpr() != null && rhs.bitExpr(0).simpleExpr() != null && rhs.bitExpr(0).simpleExpr().literals() != null && rhs.bitExpr(0).simpleExpr().literals().numberLiterals() != null && !rhs.bitExpr(0).simpleExpr().literals().numberLiterals().isEmpty()) {
            // Number
            Integer num = Integer.parseInt(rhs.getText());
            rhsExp = new C_Expression(new D_Constant(num));
        } else if (rhs.bitExpr() != null && rhs.bitExpr(0).simpleExpr() != null && rhs.bitExpr(0).simpleExpr().literals() != null && rhs.bitExpr(0).simpleExpr().literals().stringLiterals() != null && !rhs.bitExpr(0).simpleExpr().literals().stringLiterals().isEmpty()) {
            // String
            rhsExp = new C_Expression(new D_Constant(rhs.getText()));
        }

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

    // Command Attributes for Insert & Part of Update
    @Override
    public Object visitInsertIdentifier(MySQLStatementParser.InsertIdentifierContext ctx) {
        this.insertFields.add(ctx.getText());
        return super.visitInsertIdentifier(ctx);
    }

    @Override
    public Object visitNumberLiterals(MySQLStatementParser.NumberLiteralsContext ctx) {
        this.insertValues.add(new D_Constant(Integer.parseInt(ctx.getText())));
        this.updateFieldValue = new C_Expression(new D_Constant(Integer.parseInt(ctx.getText())));

        return super.visitNumberLiterals(ctx);
    }

    @Override
    public Object visitStringLiterals(MySQLStatementParser.StringLiteralsContext ctx) {
        this.insertValues.add(new D_Constant(ctx.getText()));
        this.updateFieldValue = new C_Expression(new D_Constant(ctx.getText()));

        return super.visitStringLiterals(ctx);
    }

    // Command Attributes for Update
    @Override
    public Object visitAssignment(MySQLStatementParser.AssignmentContext ctx) {
        this.updateFieldName = ctx.columnRef().getText();
        return super.visitAssignment(ctx);
    }


    // Command Create Table
    @Override
    public Object visitColumnDefinition(MySQLStatementParser.ColumnDefinitionContext ctx) {
        String fieldName = ctx.column_name.getText();
        String dataType = ctx.fieldDefinition().dataType().getText();
        if (dataType.equals("int")) {
            schema.addIntField(fieldName);
        } else if (dataType.startsWith("varchar")) {
            dataType = dataType.substring("varchar".length());
            dataType = dataType.replace("(", "");
            dataType = dataType.replace(")", "");
            int length = Integer.parseInt(dataType);
            schema.addStringField(fieldName, length);
        } else {
            throw new RuntimeException("Unsupported Column Type");
        }
        return super.visitColumnDefinition(ctx);
    }

    public Object getValue() {
        switch (commandType) {
            case QUERY:
                return new QueryData(selectFields, tableName, predicate);
            case DELETE:
                return new DeleteData(tableName, predicate);
            case CREATE_INDEX:
                return new CreateIndexData(indexName, tableName, indexFieldName);
            case INSERT:
                return new InsertData(tableName, insertFields, insertValues);
            case MODIFY:
                return new ModifyData(tableName, updateFieldName, updateFieldValue, predicate);
            case CREATE_TABLE:
                return new CreateTableData(tableName, schema);
        }
        return new QueryData(selectFields, tableName, predicate);
    }


    enum COMMAND_TYPE {
        QUERY, MODIFY, INSERT, DELETE, CREATE_TABLE, CREATE_INDEX
    }
}
