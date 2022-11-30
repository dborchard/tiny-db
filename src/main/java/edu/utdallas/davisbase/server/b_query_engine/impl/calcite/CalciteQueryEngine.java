package edu.utdallas.davisbase.server.b_query_engine.impl.calcite;

import edu.utdallas.davisbase.server.b_query_engine.IQueryEngine;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.davisbase.server.b_query_engine.common.dto.TableDto;
import edu.utdallas.davisbase.server.b_query_engine.impl.calcite.core.B_SimpleTable;
import edu.utdallas.davisbase.server.b_query_engine.impl.calcite.core.C_SimpleSchema;
import edu.utdallas.davisbase.server.b_query_engine.impl.calcite.utils.JavaSqlTypeToCalciteSqlTypeConversionRules;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.common.file.FileMgr;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordValueLayout;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class CalciteQueryEngine implements IQueryEngine {
    public static int BLOCK_SIZE = 512;
    FileMgr fm;
    MetadataMgr mdm;
    Connection connection;

    String tableName = "T1";
    String schemaName = "davisbase";

    @SneakyThrows
    public CalciteQueryEngine(String dirname) {

        //1. Init MetaDataManager (Catalog)
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, BLOCK_SIZE);
        Transaction tx1 = newTx();
        mdm = new MetadataMgr(fm.isNew(), tx1);
        tx1.commit();


        // 4.a JDBC similar
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        connection = DriverManager.getConnection("jdbc:calcite:", info);
    }


    /**
     * Use the Syntax:
     * <code>
     * select A,B from davisbase.T1;
     * </code>
     */
    @SneakyThrows
    public TableDto doQuery(String sql) {
        //2.a Get Table Layout
        Transaction tx2 = newTx();
        RecordValueLayout tableLayout = mdm.getLayout(tableName, tx2);

        // 2.b Create List<SqlType>
        JavaSqlTypeToCalciteSqlTypeConversionRules dataTypeRules = JavaSqlTypeToCalciteSqlTypeConversionRules.instance();
        List<SqlTypeName> fieldTypes = tableLayout.schema().fields().stream().map(e -> tableLayout.schema().type(e)).map(dataTypeRules::lookup).collect(Collectors.toList());

        // 2.c Create CalciteTable Object using fieldNames, fieldTypes etc
        B_SimpleTable calciteTable = new B_SimpleTable(tableName, tableLayout.schema().fields(), fieldTypes, tx2, mdm);


        // 3. Create Schema for the CalciteTable
        C_SimpleSchema schema = new C_SimpleSchema(Collections.singletonMap(tableName, calciteTable));

        // 4. Add schema to the SQL root schema

        // 4.b Unwrap and add proxy
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add(schemaName, schema);

        // 5. Execute JDBC Query
        Statement statement = calciteConnection.createStatement();
        ResultSet rs = statement.executeQuery(sql);

        List<String> columnNames = tableLayout.schema().fields();
        List<List<String>> rows = new ArrayList<>();
        while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (String field : columnNames) row.add(rs.getString(field));
            rows.add(row);
        }


        rs.close();
        statement.close();
        tx2.commit();

        return new TableDto(columnNames, rows);
    }

    @Override
    public TableDto doUpdate(String sql) {
        return null;
    }

    @SneakyThrows
    @Override
    public void close() {
        connection.close();
    }

    private Transaction newTx() {
        return new Transaction(fm);
    }

}
