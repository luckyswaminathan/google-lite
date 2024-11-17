package cis5550.flame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.HTTP;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {

    private String tableName;
    private final KVSClient kvs;
    private final String flameCoordinatorAddr;

    public FlameRDDImpl(String tableName, KVSClient _kvs, String _flameCoordinatorAddr) {
        this.tableName = tableName;
        kvs = _kvs;
        flameCoordinatorAddr = _flameCoordinatorAddr;
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> result = new ArrayList<>();
        Iterator<Row> rows = kvs.scan(tableName);

        while (rows.hasNext()) {
            Row row = rows.next();
            for (String col : row.columns()) {
                result.add(row.get(col));
            }
        }

        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String outputTable = "RDD_" + System.currentTimeMillis();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        context.invokeOperation("flatMap", serializedLambda, tableName, outputTable);

        return new FlameRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String outputTable = "PairRDD_" + System.currentTimeMillis();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        context.invokeOperation("mapToPair", serializedLambda, tableName, outputTable);

        return new FlamePairRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'intersection'");
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'sample'");
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'groupBy'");
    }

    @Override
    public int count() throws Exception {
        return kvs.count(tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        if (kvs.rename(tableName, tableNameArg)) {
            this.tableName = tableNameArg;
        } else {
            throw new IOException("Failed to rename table from " + tableName + " to " + tableNameArg);
        }
    }

    @Override
    public FlameRDD distinct() throws Exception {
        String outputTable = "Distinct_" + System.currentTimeMillis();
        Iterator<Row> rows = kvs.scan(tableName);

        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get(row.columns().iterator().next());
            kvs.put(outputTable, value, "value", value.getBytes());
        }

        return new FlameRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

    @Override
    public void destroy() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'destroy'");
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Vector<String> result = new Vector<>();
        Iterator<Row> rows = kvs.scan(tableName);

        int count = 0;
        while (rows.hasNext() && count < num) {
            Row row = rows.next();
            result.add(row.get(row.columns().iterator().next()));
            count++;
        }

        return result;
    }

    @Override
    public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
        String outputValue = zeroElement;
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        List<String> results = context.invokeOperation("fold", serializedLambda, this.tableName, "", zeroElement,
                true);

        for (String result : results) {
            outputValue = lambda.op(outputValue, result);
        }

        return outputValue;
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String outputTable = "PairRDD_" + System.currentTimeMillis();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        context.invokeOperation("flatMapToPair", serializedLambda, tableName, outputTable);

        return new FlamePairRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        String outputTable = "RDD_" + System.currentTimeMillis();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        context.invokeOperation("filter", serializedLambda, tableName, outputTable);

        return new FlameRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'mapPartitions'");
    }
}
