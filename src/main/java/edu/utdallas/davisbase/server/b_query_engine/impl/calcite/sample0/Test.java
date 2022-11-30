package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.sample0;

import edu.utdallas.davisbase.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.davisbase.server.c_key_value_store.Transaction;
import edu.utdallas.davisbase.server.d_storage_engine.common.file.FileMgr;
import edu.utdallas.davisbase.server.d_storage_engine.impl.data.page.heap.RecordValueLayout;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.File;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Test {
    public static int BLOCK_SIZE = 512;
    FileMgr fm;

    public void run(String tableName, String schemaName) throws SQLException, ClassNotFoundException {
        //1. Init MetaDataManager (Catalog)
        File dbDirectory = new File("davisdb");
        fm = new FileMgr(dbDirectory, BLOCK_SIZE);
        Transaction tx1 = newTx();
        MetadataMgr mdm = new MetadataMgr(fm.isNew(), tx1);
        tx1.commit();

        //2.a Get Table Layout
        Transaction tx2 = newTx();
        RecordValueLayout tableLayout = mdm.getLayout(tableName, tx2);

        // 2.b Create List<SqlType>
        D_SimpleDBToSqlTypeConversionRules dataTypeRules = D_SimpleDBToSqlTypeConversionRules.instance();
        List<SqlTypeName> fieldTypes = tableLayout.schema().fields().stream().map(e -> tableLayout.schema().type(e)).map(dataTypeRules::lookup).collect(Collectors.toList());

        // 2.c Create CalciteTable Object using fieldNames, fieldTypes etc
        B_SimpleTable calciteTable = new B_SimpleTable(tableName, tableLayout.schema().fields(), fieldTypes, tx2, mdm);


        // 3. Create Schema for the CalciteTable
        C_SimpleSchema schema = new C_SimpleSchema(Collections.singletonMap(tableName, calciteTable));

        // 4. Add schema to the SQL root schema

        // 4.a JDBC similar
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

        // 4.b Unwrap and add proxy
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add(schemaName, schema);

        // 5. Execute JDBC Query
        Statement statement = calciteConnection.createStatement();
        String sql = "select A,B from " + schemaName + "." + tableName + " as e where e.A=1";
        ResultSet rs = statement.executeQuery(sql);

        while (rs.next()) {
            String columnA = rs.getString("A");
            System.out.println("A " + columnA);
        }

        tx2.commit();
        rs.close();
        statement.close();
        connection.close();
    }

    private Transaction newTx() {
        return new Transaction(fm);
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        new Test().run("T1", "davisbase");

    }


}
