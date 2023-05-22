

package com.arjunsk.tiny_db.cli;

import com.arjunsk.tiny_db.cli.utils.TablePrinter;
import com.arjunsk.tiny_db.server.b_query_engine.IQueryEngine;
import com.arjunsk.tiny_db.server.b_query_engine.common.dto.TableDto;
import com.arjunsk.tiny_db.server.b_query_engine.impl.basic.BasicQueryEngine;
import java.util.Scanner;

/**
 * CLI Driver for accessing this database.
 *
 * @author Arjun Sunil Kumar
 */
public class TinyDbCli {

  public static void run(String[] args) {
    // Create Database
    String dirname = (args.length == 0) ? "tinydb" : args[0];
    IQueryEngine db = new BasicQueryEngine(dirname);

    // Parse Queries
    cliLoop(db);

    // Close Database
    db.close();
  }

  private static void cliLoop(IQueryEngine db) {
    Scanner scanner = new Scanner(System.in).useDelimiter(";");
    TablePrinter tablePrinter = new TablePrinter();
    while (true) {
      System.out.print("tinysql> ");
      String sql = scanner.next().replace("\n", " ").replace("\r", "").trim();

      TableDto result;
        if (sql.startsWith("exit")) {
            break;
        } else if (sql.startsWith("select")) {
            result = db.doQuery(sql);
        } else {
            result = db.doUpdate(sql);
        }

        if (result.message.isEmpty()) {
            tablePrinter.print(result);
        } else {
            System.out.println(result.message);
        }
    }
    scanner.close();
  }
}
