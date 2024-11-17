package cis5550.flame;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

import cis5550.kvs.*;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;
import cis5550.tools.HTTP.Response;

import java.net.URLDecoder;

public class FlameContextImpl implements FlameContext {

    private StringBuilder outputStr = new StringBuilder();
    private final KVSClient kvs;
    private final String flameCoordinatorAddr;

    public FlameContextImpl(KVSClient _kvs, String _flameCoordinatorAddr) {
        kvs = _kvs;
        flameCoordinatorAddr = _flameCoordinatorAddr;
    }

    @Override
    public FlameRDD parallelize(List<String> data) throws Exception {
        String tableName = "RDD_" + System.currentTimeMillis();

        for (int i = 0; i < data.size(); i++) {
            String key = Hasher.hash(i + "");
            kvs.put(tableName, key, "value", data.get(i));
        }

        return new FlameRDDImpl(tableName, kvs, flameCoordinatorAddr);
    }

    @Override
    public void output(String s) {
        outputStr.append(s);
    }

    public String getOutput() {
        String outStr = outputStr.toString();
        if ("".equals(outStr)) {
            return "<no output>";
        }
        return outStr;
    }

    @Override
    public KVSClient getKVS() {
        return kvs;
    }

    public void invokeOperation(String operation, byte[] lambda, String inputTable, String outputTable)
            throws Exception {
        invokeOperation(operation, lambda, inputTable, outputTable, null, false);
    }

    public List<String> invokeOperation(String operation, byte[] lambda, String inputTable, String outputTable,
            String extraString, boolean collectResults)
            throws Exception {
        Partitioner partitioner = new Partitioner();
        List<Integer> statusCodes = Collections.synchronizedList(new ArrayList<>());
        List<String> results = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < kvs.numWorkers() - 1; i++) {
            partitioner.addKVSWorker(kvs.getWorkerAddress(i), kvs.getWorkerID(i), kvs.getWorkerID(i + 1));
        }

        // last worker gets all keys >= last and < first
        partitioner.addKVSWorker(kvs.getWorkerAddress(kvs.numWorkers() - 1),
                kvs.getWorkerID(kvs.numWorkers() - 1), null);
        partitioner.addKVSWorker(kvs.getWorkerAddress(kvs.numWorkers() - 1), null, kvs.getWorkerID(0));

        String resp = new String(HTTP.doRequest("GET", "http://" + this.flameCoordinatorAddr +
                "/workers", (byte[]) null).body());
        String[] workersArray = resp.split("\n");
        int numWorkers = Integer.parseInt(workersArray[0]);

        for (int i = 0; i < numWorkers; i++) {
            partitioner.addFlameWorker(workersArray[i + 1].split(",")[1]);
        }

        Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
        CountDownLatch latch = new CountDownLatch(partitions.size());
        List<Thread> threads = new ArrayList<>();

        String path = "";
        if (operation.equals("flatMap")) {
            path = "/rdd/flatMap";
        } else if (operation.equals("flatMapToPair")) {
            path = "/rdd/flatMapToPair";
        } else if (operation.equals("pairFlatMapToPair")) {
            path = "/rdd/pairFlatMapToPair";
        } else if (operation.equals("mapToPair")) {
            path = "/rdd/mapToPair";
        } else if (operation.equals("pairFlatMap")) {
            path = "/rdd/pairFlatMap";
        } else if (operation.equals("foldByKey")) {
            path = "/rdd/foldByKey";
        } else if (operation.equals("fromTable")) {
            path = "/rdd/fromTable";
        } else if (operation.equals("join")) {
            path = "/rdd/join";
        } else if (operation.equals("fold")) {
            path = "/rdd/fold";
        } else if (operation.equals("filter")) {
            path = "/rdd/filter";
        } else {
            throw new Error("Received unexpected operation.");
        }
        final String _path = path;

        for (Partitioner.Partition partition : partitions) {
            Thread workerThread = new Thread(() -> {
                try {
                    String request = "http://" +
                            partition.assignedFlameWorker + _path + "?inputTable=" + inputTable
                            + "&outputTable=" + outputTable + "&kvsHostname=" + kvs.getCoordinator();
                    if (partition.fromKey != null) {
                        request += "&fromRowInclusive=" + partition.fromKey;
                    }
                    if (partition.toKeyExclusive != null) {
                        request += "&toRowExclusive=" + partition.toKeyExclusive;
                    }
                    if (extraString != null && (operation.equals("foldByKey") || operation.equals("fold"))) {
                        request += "&zeroElement=" + URLEncoder.encode(extraString, "UTF-8");
                    } else if (extraString != null && operation.equals("join")) {
                        request += "&inputTable2=" + URLEncoder.encode(extraString, "UTF-8");
                    }
                    Response r = HTTP.doRequest("POST", request, lambda);
                    int statusCode = r.statusCode();
                    statusCodes.add(statusCode); // Collect status codes

                    if (collectResults) {
                        results.add(new String(r.body()));
                    }

                    if (statusCode != 200) {
                        System.err.println(
                                "Error: Worker " + partition.assignedFlameWorker + " returned status " +
                                        statusCode);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
            threads.add(workerThread);
            workerThread.start();
        }

        latch.await();
        for (int code : statusCodes) {
            if (code != 200) {
                System.out.println("One or more workers returned a non-200 status.");
            }
        }
        if (collectResults) {
            return results;
        }
        return null;
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String outputTable = "fromTableOutput_" + System.currentTimeMillis();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        invokeOperation("fromTable", serializedLambda, tableName, outputTable);
        return new FlameRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

}
