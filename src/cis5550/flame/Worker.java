package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.kvs.*;
import cis5550.webserver.Request;

public class Worker extends cis5550.generic.Worker {

  public static void main(String args[]) {
    if (args.length != 2) {
      System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
      System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
    startPingThread(server, "" + port, port);
    final File myJAR = new File("__worker" + port + "-current.jar");

    port(port);

    post("/useJAR", (request, response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    post("/rdd/flatMap", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
      KVSClient client = new KVSClient(kvsHostname);

      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);
      while (rows.hasNext()) {
        try {
          Row row = rows.next();
          Iterable<String> result = lambda.op(new String(row.get(row.columns().iterator().next())));
          if (result != null) {
            for (String value : result) {
              String uniqueRowKey = row.key() + "-" + UUID.randomUUID().toString();
              client.put(outputTable, uniqueRowKey, "value", value.getBytes());
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      response.status(200, "flatMap operation completed successfully");
      return "flatMap operation completed successfully";
    });

    post("/rdd/mapToPair", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
      KVSClient client = new KVSClient(kvsHostname);

      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);
      while (rows.hasNext()) {
        try {
          Row row = rows.next();
          FlamePair result = lambda.op(new String(row.get(row.columns().iterator().next())));
          if (result != null) {
            client.put(outputTable, result._1(), row.key(), result._2().getBytes());
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      response.status(200, "mapToPair operation completed successfully");
      return "mapToPair operation completed successfully";
    });

    post("/rdd/foldByKey", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");
      String zeroElement = request.queryParams("zeroElement");
      final String unpackedZeroElement;
      try {
        unpackedZeroElement = URLDecoder.decode(zeroElement, "UTF-8");
      } catch (UnsupportedEncodingException ex) {
        response.status(500, "failed to decode zero element");
        return "failed to decode zero element";
      }

      TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      KVSClient client = new KVSClient(kvsHostname);
      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);

      while (rows.hasNext()) {
        Row row = rows.next();
        String accumulator = unpackedZeroElement;
        for (String columnName : row.columns()) {
          String columnValue = row.get(columnName);
          accumulator = lambda.op(accumulator, columnValue);
        }
        client.put(outputTable, row.key(), "result", accumulator.getBytes());
      }

      response.status(200, "foldByKey operation completed on worker.");
      return "foldByKey operation completed on worker.";
    });

    post("/rdd/fromTable", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      RowToString lambda = (RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      KVSClient client = new KVSClient(kvsHostname);

      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);
      while (rows.hasNext()) {
        try {
          Row row = rows.next();
          String result = lambda.op(row);
          if (result != null) {
            String uniqueRowKey = row.key() + "-" + UUID.randomUUID().toString();
            client.put(outputTable, uniqueRowKey, "value", result.getBytes());
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      response.status(200, "fromTable operation completed successfully");
      return "fromTable operation completed successfully";
    });

    post("/rdd/pairFlatMap", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      PairToStringIterable lambda = (PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
      KVSClient client = new KVSClient(kvsHostname);

      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);
      while (rows.hasNext()) {
        try {
          Row row = rows.next();
          for (String col : row.columns()) {
            String key = row.key();
            Iterable<String> result = lambda.op(new FlamePair(key, row.get(col)));
            if (result != null) {
              for (String val : result) {
                String uniqueCol = col + "-" + UUID.randomUUID();
                client.put(outputTable, key, uniqueCol, val.getBytes());
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      response.status(200, "PairRDD flatMap operation completed successfully");
      return "PairRDD flatMap operation completed successfully";
    });

    post("/rdd/flatMapToPair", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      StringToPairIterable lambda = (StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
      KVSClient client = new KVSClient(kvsHostname);

      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);
      while (rows.hasNext()) {
        Row row = rows.next();
        Iterable<FlamePair> pairs = lambda.op(new String(row.get(row.columns().iterator().next())));
        if (pairs != null) {
          for (FlamePair pair : pairs) {
            String uniqueColName = "col-" + UUID.randomUUID().toString();
            client.put(outputTable, pair._1(), uniqueColName, pair._2().getBytes());
          }
        }
      }

      response.status(200, "flatMapToPair operation completed successfully");
      return "flatMapToPair operation completed successfully";
    });

    post("/rdd/pairFlatMapToPair", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      PairToPairIterable lambda = (PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
      KVSClient client = new KVSClient(kvsHostname);

      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);
      while (rows.hasNext()) {
        Row row = rows.next();
        Iterable<FlamePair> pairs = lambda.op(new FlamePair(row.key(), row.get(row.columns().iterator().next())));
        if (pairs != null) {
          for (FlamePair pair : pairs) {
            String uniqueColName = "col-" + UUID.randomUUID().toString();
            client.put(outputTable, pair._1(), uniqueColName, pair._2().getBytes());
          }
        }
      }

      response.status(200, "pairFlatMapToPair operation completed successfully");
      return "pairFlatMapToPair operation completed successfully";
    });

    post("/rdd/join", (request, response) -> {
      String inputTable1 = request.queryParams("inputTable");
      String inputTable2 = request.queryParams("inputTable2");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      KVSClient client = new KVSClient(kvsHostname);
      Iterator<Row> rows1 = client.scan(inputTable1, fromRowInclusive, toRowExclusive);
      Iterator<Row> rows2 = client.scan(inputTable2, fromRowInclusive, toRowExclusive);

      Map<String, List<String>> otherTableData = new HashMap<>();
      while (rows2.hasNext()) {
        Row row = rows2.next();
        for (String col : row.columns()) {
          otherTableData.computeIfAbsent(row.key(), k -> new ArrayList<>()).add(row.get(col));
        }
      }

      while (rows1.hasNext()) {
        Row row = rows1.next();
        String key = row.key();
        if (otherTableData.containsKey(key)) {
          List<String> valuesFromOtherTable = otherTableData.get(key);
          for (String col : row.columns()) {
            String value1 = row.get(col);
            for (String value2 : valuesFromOtherTable) {
              String uniqueColName = col + "-" + value2.hashCode();
              client.put(outputTable, key, uniqueColName, (value1 + "," + value2).getBytes());
            }
          }
        }
      }

      response.status(200, "Join operation completed successfully");
      return "Join operation completed successfully";
    });

    post("/rdd/fold", (request, response) -> {
      String inputTable = request.queryParams("inputTable");
      String zeroElement = request.queryParams("zeroElement");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      KVSClient client = new KVSClient(kvsHostname);
      String accumulator = zeroElement;
      TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);

      while (rows.hasNext()) {
        Row row = rows.next();
        for (String col : row.columns()) {
          String value = row.get(col);
          accumulator = lambda.op(accumulator, value);
        }
      }

      response.status(200, "Fold operation completed successfully");
      return accumulator;
    });

    post("/rdd/filter", (request, response) -> {
      System.out.println("filter started");
      String inputTable = request.queryParams("inputTable");
      String outputTable = request.queryParams("outputTable");
      String kvsHostname = request.queryParams("kvsHostname");
      String fromRowInclusive = request.queryParams("fromRowInclusive");
      String toRowExclusive = request.queryParams("toRowExclusive");

      StringToBoolean predicate = (StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
      KVSClient client = new KVSClient(kvsHostname);

      Iterator<Row> rows = client.scan(inputTable, fromRowInclusive, toRowExclusive);
      while (rows.hasNext()) {
        Row row = rows.next();
        for (String col : row.columns()) {
          String value = row.get(col);
          if (predicate.op(value)) {
            String uniqueColName = "col-" + UUID.randomUUID().toString();
            client.put(outputTable, row.key(), uniqueColName, value.getBytes());
          }
        }
      }

      response.status(200, "filter operation completed successfully");
      return "filter operation completed successfully";
    });
  }
}
