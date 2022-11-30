package edu.utdallas.davisbase.learning.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.*;
import java.util.Properties;

/**
 * Ref: https://gist.github.com/andriika/e9f3c34c4e29ace79806af5c2f318a88
 */
public class Test {

    public static void main(String[] args) throws Exception {
        Connection connection = jdbcConnection();

        // Add our hr schema (Containing employee table) to the virtual JDBC connection
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        Schema schema = new A_CustomSchema();
        rootSchema.add("hr", schema);

        jdbcQuery(connection, calciteConnection);
    }

    private static void jdbcQuery(Connection connection, CalciteConnection calciteConnection) throws SQLException {
        Statement statement = calciteConnection.createStatement();
        ResultSet rs = statement.executeQuery("select * from hr.employees as e where e.age >= 30");

        while (rs.next()) {
            long id = rs.getLong("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            System.out.println("id: " + id + "; name: " + name + "; age: " + age);
        }

        rs.close();
        statement.close();
        connection.close();
    }

    private static Connection jdbcConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");

        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        return connection;
    }
}
