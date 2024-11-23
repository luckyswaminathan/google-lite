package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlamePairRDDImpl implements FlamePairRDD {

    private String tableName;
    private final KVSClient kvs;
    private final String flameCoordinatorAddr;

    public FlamePairRDDImpl(String tableName, KVSClient _kvs, String _flameCoordinatorAddr) {
        this.tableName = tableName;
        kvs = _kvs;
        flameCoordinatorAddr = _flameCoordinatorAddr;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> result = new ArrayList<>();
        Iterator<Row> rows = kvs.scan(tableName);

        while (rows.hasNext()) {
            Row row = rows.next();
            for (String col : row.columns()) {
                result.add(new FlamePair(row.key(), row.get(col)));
            }
        }
        return result;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String outputTable = "RDD_" + System.currentTimeMillis();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        context.invokeOperation("foldByKey", serializedLambda, tableName, outputTable, zeroElement, false);

        return new FlamePairRDDImpl(outputTable, kvs, flameCoordinatorAddr);
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
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String outputTable = "RDD_" + System.currentTimeMillis();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        context.invokeOperation("pairFlatMap", serializedLambda, tableName, outputTable);

        return new FlameRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

    @Override
    public void destroy() throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'destroy'");
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String outputTable = "PairRDD_" + System.currentTimeMillis();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        context.invokeOperation("pairFlatMapToPair", serializedLambda, tableName, outputTable);

        return new FlamePairRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        String outputTable = "Join_" + System.currentTimeMillis();
        String otherTable = ((FlamePairRDDImpl) other).getTableName();

        FlameContextImpl context = new FlameContextImpl(kvs, flameCoordinatorAddr);
        context.invokeOperation("join", null, this.tableName, outputTable, otherTable, false);

        return new FlamePairRDDImpl(outputTable, kvs, flameCoordinatorAddr);
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'cogroup'");
    }

}
