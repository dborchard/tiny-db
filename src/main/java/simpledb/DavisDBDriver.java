package simpledb;

import simpledb.a_server.SimpleDB;
import simpledb.f_tx.Transaction;

import java.util.Scanner;

public class DavisDBDriver {

    public static void main(String args[]) {
        // Create Database
        String dirname = (args.length == 0) ? "davisdb" : args[0];
        SimpleDB db = new SimpleDB(dirname);

        // Parse Queries
        parseInput(db);

        // Close Database
        db.close();
    }

    private static void parseInput(SimpleDB db) {
        Scanner scanner = new Scanner(System.in).useDelimiter(";");
        while (true) {
            System.out.print("davisql> ");
            String sql = scanner.next().replace("\n", " ").replace("\r", "").trim();

            Transaction tx = db.newTx();
            if (sql.startsWith("exit")) break;
            else if (sql.startsWith("select")) db.doQuery(sql, tx);
            else db.doUpdate(sql, tx);
            tx.commit();
        }
        scanner.close();
    }
}
