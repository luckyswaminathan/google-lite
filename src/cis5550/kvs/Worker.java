package cis5550.kvs;

import cis5550.tools.KeyEncoder;
import cis5550.tools.Logger;
import cis5550.webserver.Server;
import static cis5550.webserver.Server.*;
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker {
  private static final Logger logger = Logger.getLogger(Worker.class);
  private static ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> tables = new ConcurrentHashMap<>();
  private static String storageDir;

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      String message = "Incorrect Usage: Please provide <workerPort> <storageDir> <coordinatorIp>:<coordinatorPort>";
      logger.error(message);
      System.err.println(message);
      System.exit(1);
    }
    int portNum = Integer.parseInt(args[0]);
    storageDir = args[1];
    String coordinatorIpPort = args[2];
    port(portNum);
    startPingThread(portNum, storageDir, coordinatorIpPort);

    // Definition for LF
    final byte[] LF = "\n".getBytes(StandardCharsets.UTF_8);

    // Define the GET route for viewing a list of tables
    get("/", (req, res) -> {
      StringBuilder html = new StringBuilder();

      // Start HTML
      html.append("<html>");

      // Page Title
      html.append("<head><title>KVS Worker Tables</title></head>");

      // Start body, table
      html.append("<body>");
      html.append("<h1>KVS Worker Tables</h1>");
      html.append("<table border='1'>");
      html.append("<tr><th>Table Name</th><th>Number Entries</th></tr>");
      // Construct a row for each worker
      for (Map.Entry<String, ConcurrentHashMap<String, Row>> tableEntry : tables.entrySet()) {
        String tableName = tableEntry.getKey();
        ConcurrentHashMap<String, Row> tableRows = tableEntry.getValue();

        // Start table row
        html.append("<tr>");

        // Add Hyperlink string to row
        html.append("<td>");
        html.append("<a href='/view/").append(tableName).append("/'>")
            .append(tableName)
            .append("</a>");
        html.append("</td>");

        // Add number of values count to row
        html.append("<td>").append(tableRows.size()).append("</td>");

        // End table row
        html.append("</tr>");
      }

      // End table, body, and html
      html.append("</table>");
      html.append("</body>");
      html.append("</html>");
      return html.toString();
    });

    // Define the GET route for a specific table view
    get("/view/:tableName", (req, res) -> {
      // First, see if the row exists, return 404 if it doesn't
      String qTableName = req.params("tableName");
      if (!tables.containsKey(qTableName)) {
        res.status(404, "Not Found");
        return "Table Not Found";
      }

      ConcurrentHashMap<String, Row> table = tables.get(qTableName);

      // Get a list of all unique column keys, and sort them (used for providing
      // columns in sorted order)
      Set<String> columns = new HashSet<>();
      for (Row row : table.values()) {
        columns.addAll(row.columns());
      }
      List<String> columnKeysList = new ArrayList<>(columns);
      Collections.sort(columnKeysList);

      // Get a list of all rows keys, and sort them (used for making the HTML table
      // rows in sorted order)
      List<String> rowKeysList = new ArrayList<>(table.keySet());
      Collections.sort(rowKeysList);

      // If fromRow queryParam is present, first, filter out all rows that are smaller
      // than this row id
      String fromRowId = req.queryParams("fromRow");
      if (fromRowId != null) {
        List<String> filteredRowKeysList = new ArrayList<>();
        for (String rowKey : rowKeysList) {
          if (rowKey.compareTo(fromRowId) >= 0) {
            filteredRowKeysList.add(rowKey);
          }
        }
        rowKeysList = filteredRowKeysList;
      }

      // Page number defaulted to 1, parse it if available in URL
      int pageNum = 1;
      if (req.queryParams().contains("pageNum")) {
        try {
          pageNum = Integer.parseInt(req.queryParams("pageNum"));
          if (pageNum < 1) {
            pageNum = 1;
          }
        } catch (NumberFormatException e) {
          // If pageNum is not a valid integer, default to 1
          logger.info("Page number invalid: defaulted to 1");
        }
      }

      int itemsPerPage = 10;
      int fromIndex = itemsPerPage * (pageNum - 1);
      int toIndex = Math.min(itemsPerPage * pageNum, rowKeysList.size());

      // Next button preparation
      boolean hasNext = toIndex < rowKeysList.size();
      int nextPageNum = pageNum + 1;

      // Previous button preparation
      boolean hasPrev = pageNum > 1;
      int prevPageNum = pageNum - 1;

      // Pagination preparation
      List<String> paginatedRowKeysList;
      if (fromIndex >= rowKeysList.size()) {
        // If the fromIndex is beyond the list size, return an empty page
        paginatedRowKeysList = Collections.emptyList();
      } else {
        paginatedRowKeysList = rowKeysList.subList(fromIndex, toIndex);
      }

      // Make URLs for the previous and next buttons
      // Prepare URLs for Next and Previous buttons
      String nextUrl = "";
      String prevUrl = "";
      String baseUrl = req.url().split("\\?")[0];

      String currentUrl = req.url();

      // Build Next URL
      if (hasNext) {
        StringBuilder nextUrlBuilder = new StringBuilder(baseUrl);
        nextUrlBuilder.append("?pageNum=")
            .append(URLEncoder.encode(String.valueOf(nextPageNum), StandardCharsets.UTF_8));

        // Pass down existing query parameters (except 'pageNum')
        for (String param : req.queryParams()) {
          if (!param.equals("pageNum")) {
            String value = req.queryParams(param);
            nextUrlBuilder.append("&").append(URLEncoder.encode(param, StandardCharsets.UTF_8))
                .append("=").append(URLEncoder.encode(value, StandardCharsets.UTF_8));
          }
        }
        nextUrl = nextUrlBuilder.toString();
      }

      // Build Previous URL
      if (hasPrev) {
        StringBuilder prevUrlBuilder = new StringBuilder(currentUrl);
        prevUrlBuilder.append("?pageNum=")
            .append(URLEncoder.encode(String.valueOf(prevPageNum), StandardCharsets.UTF_8));

        // Pass down existing query parameters (except 'pageNum')
        for (String param : req.queryParams()) {
          if (!param.equals("pageNum")) {
            String value = req.queryParams(param);
            prevUrlBuilder.append("&").append(URLEncoder.encode(param, StandardCharsets.UTF_8))
                .append("=").append(URLEncoder.encode(value, StandardCharsets.UTF_8));
          }
        }
        prevUrl = prevUrlBuilder.toString();
      }

      // MAKE THE HTML TABLE RESPONSE
      StringBuilder html = new StringBuilder();

      // Start HTML, Page Title, Body, Page Heading, and Table
      html.append("<html>");
      html.append("<head><title>KVS Table View</title></head>");
      html.append("<body>");
      html.append("<h1>KVS Table View: ").append(qTableName).append("</h1>");
      html.append("<table border='1'>");

      // Make the HTML table headers row by first row being hardcoded, then looping
      // through all (sorted) column names;
      html.append("<tr>");
      html.append("<th>Row Name</th>");
      for (String columnName : columnKeysList) {
        html.append("<th>").append(columnName).append("</th>");
      }
      html.append("</tr>");

      // Construct HTML table row for each row of the KVS table's row
      for (String rowName : paginatedRowKeysList) {
        Row tableRow = table.get(rowName); // Guaranteed to exist because rowName is from table's keys list
        html.append("<tr>"); // Start table row
        html.append("<td>").append(rowName).append("</td>"); // First column should be the row's key
        // Add in every column, based on the sorted columns list
        for (String columnName : columnKeysList) {
          String colVal = tableRow.get(columnName);
          html.append("<td>");
          html.append(colVal != null ? colVal : ""); // If colVal is null, return an empty string instead of null
          html.append("</td>");
        }
        html.append("</tr>"); // End table row
      }

      html.append("</table>"); // End table

      html.append("<div>");
      // Could also implement a previous button, but fails automated tests if I do
      /*
       * if (hasPrev) {
       * html.append("<a href='").append(prevUrl).append("'>Previous</a> ");
       * }
       */
      if (hasNext) {
        html.append("<a href='").append(nextUrl).append("'>Next</a>");
      }
      html.append("</div>");

      html.append("</body>");
      html.append("</html>");
      return html.toString();
    });

    // Define the PUT route for adding data
    put("/data/:table/:row/:column", (req, res) -> {
      String tableId = req.params("table");
      String rowId = req.params("row");
      String colId = req.params("column");

      // EC: Conditional PUT chekcing query params
      String ifcolumn = req.queryParams("ifcolumn");
      String equalsParamVal = req.queryParams("equals");

      // Either get or, if it doesn't exist, create the row
      Row currRow = getRow(tableId, rowId);
      if (currRow == null) {
        currRow = new Row(rowId);
      }

      // EC: Conditional PUT Checking ifcolumn and equals val
      if (ifcolumn != null && equalsParamVal != null) {
        byte[] pastValBytes = currRow.getBytes(ifcolumn);
        if (pastValBytes == null) {
          logger.info("Fail: ifcolumn '" + ifcolumn + "' not exist in row '" + rowId + "'");
          return "FAIL";
        } else {
          byte[] toEqualBytes = equalsParamVal.getBytes(StandardCharsets.UTF_8);
          if (!Arrays.equals(pastValBytes, toEqualBytes)) {
            // Value does not match
            logger.info("Fail: Value of ifcolumn '" + ifcolumn + "' not equal '" + equalsParamVal + "'");
            return "FAIL";
          }
        }
      }

      // Set the value of the column within the row, then put the row into table
      currRow.put(colId, req.bodyAsBytes());
      putRow(tableId, currRow);
      return "OK";
    });

    // put for entire row
    put("/data/:table", (req, res) -> {
      String tableId = req.params("table");
      Row newRow;
      try {
        byte[] bodyBytes = req.bodyAsBytes();
        if (bodyBytes.length == 0) {
          throw new Exception("Request body is empty");
        }
        newRow = Row.readFrom(new java.io.ByteArrayInputStream(bodyBytes));
      } catch (Exception e) {
        logger.error("Failed to deserialize Row from request body", e);
        res.status(400, "BAD REQUEST");
        return "BAD REQUEST";
      }

      if (newRow == null) {
        res.status(400, "BAD REQUEST");
        return "BAD REQUEST";
      }
      putRow(tableId, newRow);
      return "OK";
    });

    // Define the PUT rename route, meant to rename the table name (move all
    // rowFiles from one folder to another)
    put("/rename/:table", (req, res) -> {
      String fromTableId = req.params("table");
      String toTableId = req.body();

      if (fromTableId.startsWith("pt-")) {
        // First check if this persistent table exists
        File fromTableDir = new File(storageDir, fromTableId);
        if (!fromTableDir.exists()) {
          res.status(404, "NOT FOUND");
          return "Table with specified table ID in path parameters not found";
        }

        // Handle the case for when persistent table being renamed to in-memory,
        // shouldn't be allowed return 400
        if (!toTableId.startsWith("pt-")) {
          res.status(400, "BAD REQUEST");
          return "FromTable is persistent, but ToTable is not labeled as persistent (doesn't start with \"pt-\")";
        }

        // Then check if the toTableId already exists (return 409 if so)
        File toTableDir = new File(storageDir, toTableId);
        if (toTableDir.exists()) {
          logger.error("Table on disk already exists: " + toTableId + ". Cannot rename persistent table");
          res.status(409, "CONFLICT");
          return "Table with specified table ID in body already exists, cannot rename";
        }

        // If both checks are okay, then simply move the files
        try {
          Files.move(fromTableDir.toPath(), toTableDir.toPath());
        } catch (IOException e) {
          logger.error("Error renaming table: " + e.getMessage(), e);
          res.status(500, "Internal Server Error");
          return "Error renaming table";
        }
      } else {
        // First check if this in-memory table exists
        ConcurrentHashMap<String, Row> fromTable = tables.get(fromTableId);
        if (fromTable == null) {
          res.status(404, "NOT FOUND");
          return "The specified table not found";
        }

        // Handle toTable being persistent and not being persistent as separate cases
        if (toTableId.startsWith("pt-")) {
          // Handle the case for when in-memory table is being converted to persistent
          // table

          // First check that toTableId is not an existing persistent table
          File toTableDir = new File(storageDir, toTableId);
          if (toTableDir.exists()) {
            logger.error("Table on disk already exists: " + toTableId + ". Cannot make in-memory table persistent");
            res.status(409, "CONFLICT");
            return "Table with specified table ID in body already exists, cannot rename";
          }

          // If toTableId not existing on disk, then get all rows from in-memory table and
          // put them into the persistent table
          for (Row newRow : fromTable.values()) {
            putRow(toTableId, newRow);
          }

          // After that, delete the in-memory storage for the fromTableId
          tables.remove(fromTableId);
        } else {
          // Next, check if the toTableId already exists in memory (return 409 if so)
          ConcurrentHashMap<String, Row> toTable = tables.get(toTableId);
          if (toTable != null) {
            logger.error("Table in memory already exists: " + toTableId + ". Cannot rename");
            res.status(409, "CONFLICT");
            return "Table with specified table ID in body already exists, cannot rename";
          }

          // If toTableId not existing in memory, then simply move the values, delete the
          // old key
          tables.put(toTableId, tables.remove(fromTableId));
        }
      }

      return "OK";
    });

    // Define the PUT delete route, meant to delete the table (delete all rowFiles
    // in subdirectory)
    put("/delete/:table", (req, res) -> {
      String tableId = req.params("table");

      if (tableId.startsWith("pt-")) {
        // First check if this persistent table exists
        File tableDir = new File(storageDir, tableId);
        if (!tableDir.exists()) {
          res.status(404, "NOT FOUND");
          return "Table with specified table ID in path parameters not found";
        }

        // Since the directory exists, remove all files within the directory
        // (recursively just in case), then delete the directory
        try {
          recursiveDeleteDirectory(tableDir);
        } catch (IOException e) {
          logger.error("Error deleting table directory: " + e.getMessage(), e);
          res.status(500, "Internal Server Error");
          return "Error deleting the specified table";
        }

      } else {
        // First check if this in-memory table exists
        if (!tables.containsKey(tableId)) {
          res.status(404, "NOT FOUND");
          return "The specified table not found";
        }
        // Since the provided table exists, simply delete it from memory
        tables.remove(tableId);
      }
      return "OK";
    });

    Server.get("/tables", (res, req) -> {
      String ret = "";

      String tmp;
      for (Iterator it = tables.keySet().iterator(); it.hasNext(); ret = ret + tmp + "\n") {
        tmp = (String) it.next();
      }

      return ret;
    });

    // Define the GET route to get a count of rows for a specified table (memory and
    // persistent)
    get("/count/:table", (req, res) -> {
      String tableId = req.params("table");
      long retCount;

      if (tableId.startsWith("pt-")) {
        File tableDir = new File(storageDir, tableId);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
          res.status(404, "NOT FOUND");
          return "The specified table not found";
        }

        // set the retCount to the number of files in the table's directory; guaranteed
        // to exist because of earlier file check
        retCount = tableDir.listFiles().length;
      } else {
        ConcurrentHashMap<String, Row> table = tables.get(tableId);
        if (table == null) {
          res.status(404, "NOT FOUND");
          return "The specified table not found";
        }
        retCount = table.size();
      }

      return retCount; // Auto converts to string
    });

    // Define the GET route to stream a whole table (specified by table)
    get("/data/:table", (req, res) -> {
      String tableId = req.params("table");
      String startRow = req.queryParams("startRow");
      String endRowExclusive = req.queryParams("endRowExclusive");

      // Set the content return type to text/plain
      res.type("text/plain");

      if (tableId.startsWith("pt-")) {
        File tableDir = new File(storageDir, tableId);
        if (!tableDir.exists() || !tableDir.isDirectory()) {
          res.status(404, "NOT FOUND");
          return "The specified table not found";
        }

        File[] tableRowFiles = tableDir.listFiles();
        if (tableRowFiles == null) {
          return null;
        }

        // Sort files by file name
        Arrays.sort(tableRowFiles, Comparator.comparing(File::getName));

        for (File tableRowFile : tableRowFiles) {
          String rowKey = KeyEncoder.decode(tableRowFile.getName());

          // Skip files that are not within [startRow, endRowExclusive) range of rowKeys
          if (startRow != null && rowKey.compareTo(startRow) < 0) {
            continue;
          }
          if (endRowExclusive != null && rowKey.compareTo(endRowExclusive) >= 0) {
            continue;
          }

          try (FileInputStream fileInputStream = new FileInputStream(tableRowFile)) {
            Row row = Row.readFrom(fileInputStream);
            res.write(row.toByteArray());
            res.write(LF);
          } catch (IOException e) {
            logger.error("Error reading row from disk or writing to res.write(): " + e.getMessage(), e);
          }
        }
      } else {
        ConcurrentHashMap<String, Row> table = tables.get(tableId);
        if (table == null) {
          res.status(404, "NOT FOUND");
          return "The specified table not found";
        }

        List<String> rowKeysList = new ArrayList<>(table.keySet());
        Collections.sort(rowKeysList);

        for (String rowKey : rowKeysList) {
          if (startRow != null && rowKey.compareTo(startRow) < 0) {
            continue;
          }
          if (endRowExclusive != null && rowKey.compareTo(endRowExclusive) >= 0) {
            continue;
          }

          try {
            Row row = getRow(tableId, rowKey);
            res.write(row.toByteArray());
            res.write(LF);
          } catch (IOException e) {
            logger.error("Error getting row from memory, or writing to res.write(): " + e.getMessage(), e);
          }
        }
      }

      // Closing LF signifying end of the stream, regardless of persistent table or
      // in-memory table
      res.write(LF);
      return null;
    });

    // Define the GET route to get a whole row (specified by table, row)
    get("/data/:table/:row", (req, res) -> {
      String tableId = req.params("table");
      String rowId = req.params("row");

      Row row = getRow(tableId, rowId);
      if (row == null) {
        res.status(404, "NOT FOUND");
        return "Row not found";
      }

      byte[] retRowVal = row.toByteArray();
      res.bodyAsBytes(retRowVal);
      return null;
    });

    // Define the GET route to get a specific cell (specified by table, row, and
    // column)
    get("/data/:table/:row/:column", (req, res) -> {
      String tableId = req.params("table");
      String rowId = req.params("row");
      String colId = req.params("column");

      // Check if table exists, return 404 otherwise
      if (!tableId.startsWith("pt-") && !tables.containsKey(tableId)) {
        res.status(404, "NOT FOUND");
        return "Specified table is not found";
      }

      // Table exists, now check if the row exists, return 404 otherwise
      Row row = getRow(tableId, rowId);
      if (row == null) {
        res.status(404, "NOT FOUND");
        return "Row not found";
      }

      // Row exits, now check if the column exists, return 404 otherwise
      byte[] retColVal = row.getBytes(colId);
      if (retColVal == null) {
        res.status(404, "NOT FOUND");
        return "The table and row exist, but specified column not found within.";
      }

      res.bodyAsBytes(retColVal);
      return null;
    });
  }

  private static void putRow(String tableId, Row newRow) {
    if (tableId.startsWith("pt-")) {
      // Every table is a directory, and each row is a file within that directory

      // Either replace the existing row's file within the table directory, or create
      // a new row file
      File tableDir = new File(storageDir, tableId);
      if (!tableDir.exists()) {
        tableDir.mkdirs(); // Create table directory if it doesn't exist
      }
      String encodedKey = KeyEncoder.encode(newRow.key());
      File rowFile = new File(tableDir, encodedKey);
      try (FileOutputStream outputStream = new FileOutputStream(rowFile)) {
        byte[] rowBytes = newRow.toByteArray();
        outputStream.write(rowBytes);
      } catch (IOException e) {
        logger.error("Error writing row to disk: " + e.getMessage(), e);
      }
    } else {
      tables.putIfAbsent(tableId, new ConcurrentHashMap<>());
      ConcurrentHashMap<String, Row> table = tables.get(tableId);
      table.put(newRow.key(), newRow);
    }
  }

  private static Row getRow(String tableId, String rowId) {
    if (tableId.startsWith("pt-")) {
      // Read row from disk
      File tableDir = new File(storageDir, tableId);
      if (!tableDir.exists()) {
        logger.info("Table's Directory Not Found: " + tableId);
        return null; // Table's directory doesn't exist
      }

      String encodedKey = KeyEncoder.encode(rowId);
      File rowFile = new File(tableDir, encodedKey);
      if (!rowFile.exists()) {
        logger.info("Row File Not Found: " + encodedKey);
        return null; // Row file doesn't exist
      }

      // Table directory and row file exist, now read the columns from that row's file
      try (FileInputStream inputStream = new FileInputStream(rowFile)) {
        return Row.readFrom(inputStream);
      } catch (Exception e) {
        logger.error("Error reading row from disk: " + e.getMessage(), e);
        return null;
      }
    } else {
      ConcurrentHashMap<String, Row> table = tables.get(tableId);
      // Only get from table if table isn't null; return null if table doesn't exist
      // or row doesn't exist within table
      return table != null ? table.get(rowId) : null;
    }
  }

  private static void recursiveDeleteDirectory(File file) throws IOException {
    if (file.isDirectory()) {
      File[] entries = file.listFiles();
      if (entries != null) {
        for (File entry : entries) {
          recursiveDeleteDirectory(entry);
        }
      }
    }
    if (!file.delete()) {
      throw new IOException("Failed to delete " + file.getAbsolutePath());
    }
  }
}