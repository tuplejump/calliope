package com.imaginea.cassandra.triggers;

import com.imaginea.zmq.Publisher;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * User: Suresh
 * Date: 13/8/13
 * Time: 11:04 AM
 */

@Aspect
public class StorageProxyTriggerAspect {

    private static Logger logger = LoggerFactory.getLogger(StorageProxyTriggerAspect.class);

    @Around("execution(* org.apache.cassandra.service.StorageProxy.mutateAtomically(..)) " +
            "|| execution(* org.apache.cassandra.service.StorageProxy.mutate(..))")
    public void writeToCommitLog(ProceedingJoinPoint thisJoinPoint) throws Throwable
    {
        logger.info("ASPECT HANDLER CALLED");

        try
        {
            ConsistencyLevel consistencyLevel = (ConsistencyLevel) thisJoinPoint.getArgs()[1];
            @SuppressWarnings("unchecked")
            List<IMutation> mutations = (List<IMutation>) thisJoinPoint.getArgs()[0];
            writePending(consistencyLevel, mutations);
            thisJoinPoint.proceed(thisJoinPoint.getArgs());
        }
        catch (InvalidRequestException e)
        {

        }

    }

    private void writePending(ConsistencyLevel consistencyLevel, List<IMutation> mutations) throws CharacterCodingException {
        for (IMutation mutation : mutations) {
            if (mutation instanceof RowMutation) {
                RowMutation rowMutation = (RowMutation) mutation;
                logger.debug("Mutation for [" + rowMutation.getTable() + "] with consistencyLevel [" + consistencyLevel
                        + "]");
                writePending(consistencyLevel, rowMutation);
            }
        }
    }

    public void writePending(ConsistencyLevel consistencyLevel, RowMutation rowMutation) throws CharacterCodingException {
        List<String> columnNames = new ArrayList<String>();
        for (ColumnFamily cf : rowMutation.getColumnFamilies()) {
            for (ByteBuffer b : cf.getColumnNames()) {
                columnNames.add(ByteBufferUtil.string(b));
            }
        }
        String keyspace = rowMutation.getTable();
        ByteBuffer rowKey = rowMutation.key();
        for (UUID cfId : rowMutation.getColumnFamilyIds()) {
            ColumnFamily columnFamily = rowMutation.getColumnFamily(cfId);
            String path = keyspace + ":" + columnFamily.metadata().cfName;
            String message =  "row key ["+ rowKey + "] columnNames ["+columnNames+"] path [" + path + "]";
            publishMessage(message);
            logger.debug("row key ["+ rowKey + "] columnNames ["+columnNames+"] path [" + path + "]");
        }

    }

    private void publishMessage(String message)
    {
        logger.debug("publisher called "+message);
        Publisher publisher = Publisher.getPublisher();
        publisher.publish(message);

    }
}