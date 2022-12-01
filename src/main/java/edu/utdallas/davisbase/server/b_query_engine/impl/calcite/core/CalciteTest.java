package edu.utdallas.davisbase.server.b_query_engine.impl.calcite.core;

import edu.utdallas.davisbase.cli.utils.TablePrinter;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.MetadataMgr;
import edu.utdallas.davisbase.server.b_query_engine.common.catalog.table.domain.TablePhysicalLayout;
import edu.utdallas.davisbase.server.b_query_engine.common.dto.TableDto;
import edu.utdallas.davisbase.server.d_storage_engine.common.file.FileMgr;
import edu.utdallas.davisbase.server.d_storage_engine.common.transaction.Transaction;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Testing Calcite Overall Flow
 *
 * @author Arjun Sunil Kumar
 */
public class CalciteTest {

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
    TablePhysicalLayout tableLayout = mdm.getLayout(tableName, tx2);

    // 2.b Create List<SqlType>
    D_JavaSqlTypeToCalciteSqlTypeConversionRules dataTypeRules = D_JavaSqlTypeToCalciteSqlTypeConversionRules.instance();
    List<SqlTypeName> fieldTypes = tableLayout.schema().fields().stream()
        .map(e -> tableLayout.schema().type(e)).map(dataTypeRules::lookup)
        .collect(Collectors.toList());

    // 2.c Create CalciteTable Object using fieldNames, fieldTypes etc
    B_Table calciteTable = new B_Table(tableName, tableLayout.schema().fields(), fieldTypes, tx2,
        mdm);

    // 3. Create Schema for the CalciteTable
    C_Schema schema = new C_Schema(Collections.singletonMap(tableName, calciteTable));

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
    String sql = "select A,B from " + schemaName + "." + tableName + " as e";
    ResultSet rs = statement.executeQuery(sql);

    // 6. Print
    List<String> columnNames = tableLayout.schema().fields();
    List<List<String>> rows = new ArrayList<>();
    while (rs.next()) {
      List<String> row = new ArrayList<>();
        for (String field : columnNames) {
            row.add(rs.getString(field));
        }
      rows.add(row);
    }
    TableDto result = new TableDto(columnNames, rows);
    new TablePrinter().print(result);

    // 7. Close
    tx2.commit();
    rs.close();
    statement.close();
    connection.close();
  }

  private Transaction newTx() {
    return new Transaction(fm);
  }

  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    new CalciteTest().run("T1", "davisbase");

  }


}
