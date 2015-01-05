package com.tuplejump.calliope.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 **/
public class SplitsManager {

    private static final Logger logger = LoggerFactory.getLogger(SplitsManager.class);

    private static final SplitsManager instance = new SplitsManager();

    public static SplitsManager getInstance() {
        return instance;
    }

    private Map<String, List<TokenRange>> tokenRangeMap = new HashMap<String, List<TokenRange>>();
    private Map<String, List<InputSplit>> keyspaceSplitMap = new HashMap<String, List<InputSplit>>();

    public void refreshRange() {
        tokenRangeMap = new HashMap<String, List<TokenRange>>();
        keyspaceSplitMap = new HashMap<String, List<InputSplit>>();
    }

    public List<InputSplit> getSplits(Configuration conf) throws IOException {
        String initialAddress = ConfigHelper.getInputInitialAddress(conf);
        int rpcPort = ConfigHelper.getInputRpcPort(conf);
        String keyspace = ConfigHelper.getInputKeyspace(conf);
        String cfName = ConfigHelper.getInputColumnFamily(conf);

        String key = initialAddress + ":" + rpcPort + "://" + keyspace;


        KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
        //If the request is restricted for tokens
        if (jobKeyRange != null && jobKeyRange.start_token != null) {
            if (!(tokenRangeMap.containsKey(key))) {
                // cannonical ranges and nodes holding
                tokenRangeMap.put(key, getRangeMap(conf));
            }
            return computeSplitsWithJobKeyRange(conf, tokenRangeMap.get(key), jobKeyRange);
        } else {
            if (!keyspaceSplitMap.containsKey(key)) {

                if (!(tokenRangeMap.containsKey(key))) {
                    // cannonical ranges and nodes holding
                    tokenRangeMap.put(key, getRangeMap(conf));
                }
                keyspaceSplitMap.put(key, computeSplitsWithoutJobKeyRange(conf, tokenRangeMap.get(key)));
            }

            return keyspaceSplitMap.get(key);
        }
    }

    private List<InputSplit> computeSplitsWithJobKeyRange(Configuration conf, List<TokenRange> masterRangeNodes, KeyRange jobKeyRange) throws IOException {

        IPartitioner partitioner = ConfigHelper.getInputPartitioner(conf);

        // cannonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = new ThreadPoolExecutor(0, 128, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        List<InputSplit> splits = new ArrayList<InputSplit>();

        try {
            List<Future<List<ColumnFamilySplit>>> splitfutures = new ArrayList<Future<List<ColumnFamilySplit>>>();
            Range<Token> jobRange = null;
            jobRange = new Range<>(partitioner.getTokenFactory().fromString(jobKeyRange.start_token),
                    partitioner.getTokenFactory().fromString(jobKeyRange.end_token),
                    partitioner);

            for (TokenRange range : masterRangeNodes) {
                Range<Token> dhtRange = new Range<Token>(partitioner.getTokenFactory().fromString(range.start_token),
                        partitioner.getTokenFactory().fromString(range.end_token),
                        partitioner);

                if (dhtRange.intersects(jobRange)) {
                    for (Range<Token> intersection : dhtRange.intersectionWith(jobRange)) {
                        range.start_token = partitioner.getTokenFactory().toString(intersection.left);
                        range.end_token = partitioner.getTokenFactory().toString(intersection.right);
                        // for each range, pick a live owner and ask it to compute bite-sized splits
                        splitfutures.add(executor.submit(new SplitCallable(range, conf)));
                    }
                }
            }

            // wait until we have all the results back
            for (Future<List<ColumnFamilySplit>> futureInputSplits : splitfutures) {
                try {
                    List<ColumnFamilySplit> allSplits = futureInputSplits.get();
                    splits.addAll(allSplits);
                } catch (Exception e) {
                    logger.warn("Error reading format", e);
                    throw new IOException("Could not get input splits", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        //Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    private List<InputSplit> computeSplitsWithoutJobKeyRange(Configuration conf, List<TokenRange> masterRangeNodes) throws IOException {

        IPartitioner partitioner = ConfigHelper.getInputPartitioner(conf);

        // cannonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = new ThreadPoolExecutor(0, 128, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        List<InputSplit> splits = new ArrayList<InputSplit>();

        try {
            List<Future<List<ColumnFamilySplit>>> splitfutures = new ArrayList<Future<List<ColumnFamilySplit>>>();
            Range<Token> jobRange = null;

            for (TokenRange range : masterRangeNodes) {
                // for each range, pick a live owner and ask it to compute bite-sized splits
                splitfutures.add(executor.submit(new SplitCallable(range, conf)));
            }

            // wait until we have all the results back
            for (Future<List<ColumnFamilySplit>> futureInputSplits : splitfutures) {
                try {
                    List<ColumnFamilySplit> allSplits = futureInputSplits.get();
                    splits.addAll(allSplits);
                } catch (Exception e) {
                    logger.warn("Error reading format", e);
                    throw new IOException("Could not get input splits", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        //Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }


    protected List<TokenRange> getRangeMap(Configuration conf) throws IOException {
        Cassandra.Client client = ConfigHelper.getClientFromInputAddressList(conf);

        List<TokenRange> map;
        try {
            map = client.describe_local_ring(ConfigHelper.getInputKeyspace(conf));
        } catch (InvalidRequestException e) {
            logger.error("Error fetching ring information from host: " + ConfigHelper.getInputInitialAddress(conf) + "and port: " + ConfigHelper.getInputRpcPort(conf));
            logger.error("Cassandra Client location: " + Cassandra.Client.class.getProtectionDomain().getCodeSource().getLocation());
            logger.error("Thrift Client location: " + TBase.class.getProtectionDomain().getCodeSource().getLocation());
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return map;
    }


    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<ColumnFamilySplit>> {

        private final TokenRange range;
        private final Configuration conf;

        public SplitCallable(TokenRange tr, Configuration conf) {
            this.range = tr;
            this.conf = conf;
        }

        public List<ColumnFamilySplit> call() throws Exception {
            String keyspace = ConfigHelper.getInputKeyspace(conf);
            String cfName = ConfigHelper.getInputColumnFamily(conf);
            IPartitioner partitioner = ConfigHelper.getInputPartitioner(conf);

            ArrayList<ColumnFamilySplit> splits = new ArrayList<ColumnFamilySplit>();
            List<CfSplit> subSplits = getSubSplits(keyspace, cfName, range, conf);
            assert range.rpc_endpoints.size() == range.endpoints.size() : "rpc_endpoints size must match endpoints size";
            // turn the sub-ranges into InputSplits
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);

            int endpointIndex = 0;
            for (String endpoint : range.rpc_endpoints) {
                String endpoint_address = endpoint;
                if (endpoint_address == null || endpoint_address.equals("0.0.0.0"))
                    endpoint_address = range.endpoints.get(endpointIndex);
                endpoints[endpointIndex++] = endpoint_address;
            }

            Token.TokenFactory factory = partitioner.getTokenFactory();
            for (CfSplit subSplit : subSplits) {
                Token left = factory.fromString(subSplit.getStart_token());
                Token right = factory.fromString(subSplit.getEnd_token());
                Range<Token> range = new Range<Token>(left, right, partitioner);
                List<Range<Token>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);
                for (Range<Token> subrange : ranges) {
                    ColumnFamilySplit split =
                            new ColumnFamilySplit(
                                    factory.toString(subrange.left),
                                    factory.toString(subrange.right),
                                    subSplit.getRow_count(),
                                    endpoints);

                    logger.debug("adding " + split);
                    splits.add(split);
                }
            }
            return splits;
        }
    }

    private List<CfSplit> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf) throws IOException {
        int splitsize = ConfigHelper.getInputSplitSize(conf);
        for (int i = 0; i < range.rpc_endpoints.size(); i++) {
            String host = range.rpc_endpoints.get(i);

            if (host == null || host.equals("0.0.0.0"))
                host = range.endpoints.get(i);

            //System.out.println(String.format("RANGE: %s - %s ON %s", range.start_token, range.end_token, range.endpoints));
            try {
                Cassandra.Client client = ConfigHelper.createConnection(conf, host, ConfigHelper.getInputRpcPort(conf));
                client.set_keyspace(keyspace);

                try {
                    List<CfSplit> cfs = client.describe_splits_ex(cfName, range.start_token, range.end_token, splitsize);
                    return cfs;
                } catch (TApplicationException e) {
                    // fallback to guessing split size if talking to a server without describe_splits_ex method
                    if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                        List<String> splitPoints = client.describe_splits(cfName, range.start_token, range.end_token, splitsize);
                        return tokenListToSplits(splitPoints, splitsize);
                    }
                    throw e;
                }
            } catch (IOException e) {
                logger.debug("failed connect to endpoint " + host, e);
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IOException("failed connecting to all endpoints " + StringUtils.join(range.endpoints, ","));
    }

    private List<CfSplit> tokenListToSplits(List<String> splitTokens, int splitsize) {
        List<CfSplit> splits = Lists.newArrayListWithExpectedSize(splitTokens.size() - 1);
        for (int j = 0; j < splitTokens.size() - 1; j++)
            splits.add(new CfSplit(splitTokens.get(j), splitTokens.get(j + 1), splitsize));
        return splits;
    }
}
