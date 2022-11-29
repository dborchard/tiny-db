package edu.utdallas.davisbase.cli;

import edu.utdallas.davisbase.cli.utils.TablePrinter;
import edu.utdallas.davisbase.server.b_query_engine.impl.basic.BasicQueryEngine;
import edu.utdallas.davisbase.server.b_query_engine.common.TableDto;

import java.util.Scanner;

public class DavisCLI {

    public static void run(String[] args) {
        // Create Database
        String dirname = (args.length == 0) ? "davisdb" : args[0];
        BasicQueryEngine db = new BasicQueryEngine(dirname);

        // Parse Queries
        cliLoop(db);

        // Close Database
        db.close();
    }

    private static void cliLoop(BasicQueryEngine db) {
        Scanner scanner = new Scanner(System.in).useDelimiter(";");
        TablePrinter tablePrinter = new TablePrinter();
        while (true) {
            System.out.print("davisql> ");
            String sql = scanner.next().replace("\n", " ").replace("\r", "").trim();

            TableDto result;
            if (sql.startsWith("exit")) break;
            else if (sql.startsWith("select")) result = db.doQuery(sql);
            else result = db.doUpdate(sql);

            if (result.message.isEmpty()) tablePrinter.print(result);
            else System.out.println(result.message);
        }
        scanner.close();
    }
}
