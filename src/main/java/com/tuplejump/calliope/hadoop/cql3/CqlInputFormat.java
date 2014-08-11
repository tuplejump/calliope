/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tuplejump.calliope.hadoop.cql3;

import com.datastax.driver.core.Row;
import com.tuplejump.calliope.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 * <p/>
 * At minimum, you need to set the KS and CF in your Hadoop job Configuration.
 * The ConfigHelper class is provided to make this
 * simple:
 * ConfigHelper.setInputColumnFamily
 * <p/>
 * You can also configure the number of rows per InputSplit with
 * ConfigHelper.setInputSplitSize. The default split size is 64k rows.
 * <p/>
 * the number of CQL rows per page
 * CQLConfigHelper.setInputCQLPageRowSize. The default page row size is 1000. You
 * should set it to "as big as possible, but no bigger." It set the LIMIT for the CQL
 * query, so you need set it big enough to minimize the network overhead, and also
 * not too big to avoid out of memory issue.
 * <p/>
 * other native protocol connection parameters in CqlConfigHelper
 */
public class CqlInputFormat extends AbstractColumnFamilyInputFormat<Long, Row> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractColumnFamilyInputFormat.class);

    public RecordReader<Long, Row> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf jobConf, final Reporter reporter)
            throws IOException {
        TaskAttemptContext tac = HadoopCompat.newMapContext(
                jobConf,
                TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID)),
                null,
                null,
                null,
                new ReporterWrapper(reporter),
                null);

        CqlRecordReader recordReader = new CqlRecordReader();
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit) split, tac);
        return recordReader;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<Long, Row> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new CqlRecordReader();
    }


    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = super.getSplits(context);

        Configuration jobConf = HadoopCompat.getConfiguration(context);

        Map<String, TokenRangesWithLocations> tokenGroups = new HashMap<>();

        if (CqlConfigHelper.getMultiRangeInputSplit(jobConf)) {
            int rangesPerSplit = CqlConfigHelper.getRangesInMultiRangeSplit(jobConf);
            List<InputSplit> multiRangeSplits = new ArrayList<>();

            for (InputSplit split : splits) {
                ColumnFamilySplit cfs = (ColumnFamilySplit) split;
                String firstLocation = cfs.getLocations()[0];
                if (tokenGroups.containsKey(firstLocation)) {
                    TokenRangesWithLocations trs = tokenGroups.get(firstLocation);
                    trs.tokenRanges.add(new TokenRangeHolder(cfs.getStartToken(), cfs.getEndToken(), cfs.getLength()));
                    tokenGroups.put(firstLocation, trs);
                } else {
                    TokenRangesWithLocations trs = new TokenRangesWithLocations();
                    trs.locations = cfs.getLocations();
                    trs.tokenRanges = new ArrayList<>();
                    trs.tokenRanges.add(new TokenRangeHolder(cfs.getStartToken(), cfs.getEndToken(), cfs.getLength()));
                    tokenGroups.put(firstLocation, trs);
                }
            }

            for (Map.Entry<String, TokenRangesWithLocations> group : tokenGroups.entrySet()) {
                int vnodesAdded = 0;
                List<TokenRangeHolder> tranges = group.getValue().tokenRanges;
                int remaining = tranges.size();
                String[] endpoints = group.getValue().locations;

                while (remaining > 0) {
                    List<TokenRangeHolder> tokens;
                    if (remaining >= rangesPerSplit) {
                        tokens = tranges.subList(vnodesAdded, vnodesAdded + rangesPerSplit);
                    } else {
                        tokens = tranges.subList(vnodesAdded, tranges.size());
                    }
                    MultiRangeSplit multiRange = tokenRangesToMultiRangeSplit(endpoints, tokens);
                    multiRangeSplits.add(multiRange);
                    remaining -= tokens.size();
                    vnodesAdded += tokens.size();
                }
            }
            logger.info(String.format("Created %d Multirange Splits", multiRangeSplits.size()));
            return multiRangeSplits;
        } else {
            return splits;
        }
    }

    private MultiRangeSplit tokenRangesToMultiRangeSplit(String[] endpoints, List<TokenRangeHolder> tokens) {
        long multiRangeLength = 0;
        for (TokenRangeHolder t : tokens) {
            multiRangeLength += t.getLength();
        }
        TokenRangeHolder[] tokenArray = new TokenRangeHolder[tokens.size()];
        tokens.toArray(tokenArray);
        return new MultiRangeSplit(tokenArray, multiRangeLength, endpoints);
    }

    @Override
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        TaskAttemptContext tac = HadoopCompat.newTaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        org.apache.hadoop.mapred.InputSplit[] oldInputSplits = new org.apache.hadoop.mapred.InputSplit[newInputSplits.size()];
        for (int i = 0; i < newInputSplits.size(); i++)
            oldInputSplits[i] = (MultiRangeSplit) newInputSplits.get(i);
        return oldInputSplits;
    }

    private class TokenRangesWithLocations {
        List<TokenRangeHolder> tokenRanges;
        String[] locations;
    }
}
