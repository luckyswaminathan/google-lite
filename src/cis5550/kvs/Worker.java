
package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import cis5550.tools.KeyEncoder;

public class Worker extends cis5550.generic.Worker {

  private static Map<String, Map<String, Row>> kvs = new ConcurrentHashMap<>();
  private static String storageDirectory;

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Usage: java Worker <port> <storageDirectory> <coordinatorPort>");
      System.exit(1);
    }

    int portNumber = Integer.parseInt(args[0]);
    storageDirectory = args[1];
    String[] coordinatorInfo = args[2].split(":");
    String coordinatorIP = coordinatorInfo[0];
    int coordinatorPort = Integer.parseInt(coordinatorInfo[1]);

    port(portNumber);

    String workerID = getWorkerID(storageDirectory);

    startPingThread(portNumber, coordinatorIP, coordinatorPort, workerID);

    put("/data/:table/:row/:column", (req, res) -> {
      String tableName = req.params("table");
      String rowKey = req.params("row");
      String columnKey = req.params("column");

      if (tableName.startsWith("pt-")) {
        // persistent table
        File tableDir = new File(storageDirectory, tableName);
        if (!tableDir.exists()) {
          tableDir.mkdirs();
        }

        File rowFile = new File(tableDir, KeyEncoder.encode(rowKey));
        Row row;
        if (rowFile.exists()) {
          try (BufferedReader reader = new BufferedReader(new FileReader(rowFile))) {
            row = Row.fromByteArray(reader.readLine().getBytes());
          } catch (IOException e) {
            e.printStackTrace();
            res.status(500, "Internal Server Error");
            return "Internal Server Error";
          }
        } else {
          row = new Row(rowKey);
        }

        row.put(columnKey, req.bodyAsBytes());

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(rowFile))) {
          writer.write(new String(row.toByteArray()));
        } catch (IOException e) {
          e.printStackTrace();
          res.status(500, "Internal Server Error");
          return "Internal Server Error";
        }

        return "OK";
      } else {
        // non persistent
        Map<String, Row> newTable = new ConcurrentHashMap<>();
        Row newRow = new Row(rowKey);
        newTable.put(rowKey, newRow);

        Map<String, Row> table = kvs.putIfAbsent(tableName, newTable);
        if (table == null) {
          table = newTable;
        } else {
          Row row = table.get(rowKey);
          if (row == null) {
            row = new Row(rowKey);
            table.put(rowKey, row);
          }
        }

        Row row = table.get(rowKey);
        row.put(columnKey, req.bodyAsBytes());

        return "OK";
      }
    });

    get("/data/:table/:row/:column", (req, res) -> {
      String tableName = req.params("table");
      String rowKey = req.params("row");
      String columnKey = req.params("column");

      Row row = getRow(tableName, rowKey);
      if (row == null || row.get(columnKey) == null) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      res.bodyAsBytes(row.getBytes(columnKey));
      return null;
    });
    get("/", (req, res) -> {
      System.out.println("'get /'");
      StringBuilder html = new StringBuilder(
          "<html><body><table border='1'><tr><th>Table Name</th><th>Number of Keys</th></tr>");
      for (Map.Entry<String, Map<String, Row>> entry : kvs.entrySet()) {
        String tableName = entry.getKey();
        int numberOfKeys = entry.getValue().size();
        html.append("<tr><td><a href='/view/").append(tableName).append("'>").append(tableName).append("</a></td><td>")
            .append(numberOfKeys).append("</td></tr>");
      }
      html.append("</table></body></html>");
      return html.toString();
    });

    get("/view/:table", (req, res) -> {
      String tableName = req.params("table");
      Map<String, Row> table = kvs.get(tableName);
      if (table == null) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      int start = req.queryParams("start") != null ? Integer.parseInt(req.queryParams("start")) : 0;
      int end = start + 10;

      StringBuilder html = new StringBuilder("<html><body>");
      html.append("<h1>Table: ").append(tableName).append("</h1>");
      html.append("<table border='1'><tr><th>Row Key</th>");

      Set<String> columnNames = new TreeSet<>();
      table.values().stream().skip(start).limit(10).forEach(row -> columnNames.addAll(row.columns()));
      columnNames.forEach(columnName -> html.append("<th>").append(columnName).append("</th>"));
      html.append("</tr>");

      table.entrySet().stream().sorted(Map.Entry.comparingByKey()).skip(start).limit(10).forEach(entry -> {
        String rowKey = entry.getKey();
        Row row = entry.getValue();
        html.append("<tr><td>").append(rowKey).append("</td>");
        columnNames.forEach(columnName -> {
          String value = row.get(columnName);
          html.append("<td>").append(value != null ? new String(value) : "").append("</td>");
        });
        html.append("</tr>");
      });

      html.append("</table>");

      if (table.size() > end) {
        html.append("<a href='/view/").append(tableName).append("?start=").append(end).append("'>Next</a>");
      }

      html.append("</body></html>");
      return html.toString();
    });

    get("/data/:table/:row", (req, res) -> {
      String tableName = req.params("table");
      String rowKey = req.params("row");

      Row row = getRow(tableName, rowKey);
      if (row == null) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      res.bodyAsBytes(row.toByteArray());
      res.status(200, "OK");
      return new String(row.toByteArray());
    });

    get("/data/:table", (req, res) -> {
      String tableName = req.params("table");
      Map<String, Row> table = kvs.get(tableName);

      if (table == null) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      String startRow = req.queryParams("startRow");
      String endRowExclusive = req.queryParams("endRowExclusive");

      StringBuilder response = new StringBuilder();
      table.entrySet().stream()
          .filter(entry -> (startRow == null || entry.getKey().compareTo(startRow) >= 0) &&
              (endRowExclusive == null || entry.getKey().compareTo(endRowExclusive) < 0))
          .forEach(entry -> {
            response.append(new String(entry.getValue().toByteArray())).append("\n");
          });

      response.append("\n"); // End of stream indicator
      res.body(response.toString());
      return null;
    });

    get("/count/:table", (req, res) -> {
      String tableName = req.params("table");
      Map<String, Row> table = kvs.get(tableName);

      if (table == null) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      res.body(Integer.toString(table.size()));
      return null;
    });

    put("/rename/:oldTableName", (req, res) -> {
      String oldTableName = req.params("oldTableName");
      String newTableName = req.body();

      if (oldTableName.startsWith("pt-") && !newTableName.startsWith("pt-")) {
        res.status(400, "Bad Request");
        return "Bad Request";
      }

      Map<String, Row> table = kvs.get(oldTableName);
      if (table == null) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      if (kvs.containsKey(newTableName)) {
        res.status(409, "Conflict");
        return "Conflict";
      }

      kvs.put(newTableName, table);
      kvs.remove(oldTableName);

      if (oldTableName.startsWith("pt-")) {
        File oldTableDir = new File(storageDirectory, oldTableName);
        File newTableDir = new File(storageDirectory, newTableName);
        if (!oldTableDir.renameTo(newTableDir)) {
          res.status(500, "Internal Server Error");
          return "Internal Server Error";
        }
      }

      res.status(200, "OK");
      return "OK";
    });

    put("/delete/:table", (req, res) -> {
      String tableName = req.params("table");

      if (tableName.startsWith("pt-")) {
        File tableDir = new File(storageDirectory, tableName);
        if (tableDir.exists()) {
          for (File file : tableDir.listFiles()) {
            file.delete();
          }
          tableDir.delete();
        } else {
          res.status(404, "Not Found");
          return "Not Found";
        }
      } else {
        Map<String, Row> table = kvs.remove(tableName);
        if (table == null) {
          res.status(404, "Not Found");
          return "Not Found";
        }
      }

      res.status(200, "OK");
      return "OK";
    });
  }

  private static Row getRow(String tableName, String rowKey) {
    Map<String, Row> table = kvs.get(tableName);
    System.out.println("table: " + table);
    if (table == null) {
      if (tableName.startsWith("pt-")) {
        // persistebt table
        File tableDir = new File(storageDirectory, tableName);
        if (!tableDir.exists()) {
          return null;
        }

        File rowFile = new File(tableDir, KeyEncoder.encode(rowKey));
        if (!rowFile.exists()) {
          return null;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(rowFile))) {
          return Row.fromByteArray(reader.readLine().getBytes());
        } catch (IOException e) {
          e.printStackTrace();
          return null;
        }
      } else {
        return null;
      }
    }
    // non persistent
    return table.get(rowKey);
  }

  private static void putRow(String tableName, String rowKey, Row row) {
    Map<String, Row> table = kvs.putIfAbsent(tableName, new ConcurrentHashMap<>());
    if (table == null) {
      table = kvs.get(tableName);
    }
    table.put(rowKey, row);
  }

  private static String getWorkerID(String storageDirectory) {
    File idFile = new File(storageDirectory, "id");
    if (idFile.exists()) {
      try (BufferedReader reader = new BufferedReader(new FileReader(idFile))) {
        return reader.readLine();
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }
    } else {
      String workerID = generateRandomID();
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(idFile))) {
        writer.write(workerID);
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }
      return workerID;
    }
    return null;
  }

  private static String generateRandomID() {
    String alphabet = "abcdefghijklmnopqrstuvwxyz";
    StringBuilder sb = new StringBuilder(5);
    Random random = new Random();
    for (int i = 0; i < 5; i++) {
      sb.append(alphabet.charAt(random.nextInt(alphabet.length())));
    }
    return sb.toString();
  }
}
