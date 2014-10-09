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
package com.tuplejump.calliope.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;


public abstract class AbstractColumnFamilyInputFormat<K, Y> extends InputFormat<K, Y> implements org.apache.hadoop.mapred.InputFormat<K, Y> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractColumnFamilyInputFormat.class);

    public static final String MAPRED_TASK_ID = "mapred.task.id";
    // The simple fact that we need this is because the old Hadoop API wants us to "write"
    // to the key and value whereas the new asks for it.
    // I choose 8kb as the default max key size (instanciated only once), but you can
    // override it in your jobConf with this setting.
    public static final String CASSANDRA_HADOOP_MAX_KEY_SIZE = "cassandra.hadoop.max_key_size";
    public static final int CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;

    private String keyspace;
    private String cfName;
    private IPartitioner partitioner;

    protected void validateConfiguration(Configuration conf) {
        if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null) {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setInputColumnFamily()");
        }
        if (ConfigHelper.getInputInitialAddress(conf) == null)
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node with setInputInitialAddress");
        if (ConfigHelper.getInputPartitioner(conf) == null)
            throw new UnsupportedOperationException("You must set the Cassandra partitioner class with setInputPartitioner");
    }

    public static Cassandra.Client createAuthenticatedClient(String location, int port, Configuration conf) throws Exception {
        logger.debug("Creating authenticated client for CF input format");
        TTransport transport;
        try {
            transport = ConfigHelper.getClientTransportFactory(conf).openTransport(location, port);
        } catch (Exception e) {
            throw new TTransportException("Failed to open a transport to " + location + ":" + port + ".", e);
        }
        TProtocol binaryProtocol = new TBinaryProtocol(transport, true, true);
        Cassandra.Client client = new Cassandra.Client(binaryProtocol);

        // log in
        client.set_keyspace(ConfigHelper.getInputKeyspace(conf));
        if ((ConfigHelper.getInputKeyspaceUserName(conf) != null) && (ConfigHelper.getInputKeyspacePassword(conf) != null)) {
            Map<String, String> creds = new HashMap<String, String>();
            creds.put(IAuthenticator.USERNAME_KEY, ConfigHelper.getInputKeyspaceUserName(conf));
            creds.put(IAuthenticator.PASSWORD_KEY, ConfigHelper.getInputKeyspacePassword(conf));
            AuthenticationRequest authRequest = new AuthenticationRequest(creds);
            client.login(authRequest);
        }
        logger.debug("Authenticated client for CF input format created successfully");
        return client;
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = HadoopCompat.getConfiguration(context);

        validateConfiguration(conf);

        keyspace = ConfigHelper.getInputKeyspace(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        partitioner = ConfigHelper.getInputPartitioner(conf);
        logger.debug("partitioner is " + partitioner);

        return SplitsManager.getInstance().getSplits(conf);
    }

    //
    // Old Hadoop API
    //
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        TaskAttemptContext tac = HadoopCompat.newTaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        org.apache.hadoop.mapred.InputSplit[] oldInputSplits = new org.apache.hadoop.mapred.InputSplit[newInputSplits.size()];
        for (int i = 0; i < newInputSplits.size(); i++)
            oldInputSplits[i] = (ColumnFamilySplit) newInputSplits.get(i);
        return oldInputSplits;
    }
}
