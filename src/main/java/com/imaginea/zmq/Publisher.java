package com.imaginea.zmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.io.IOException;

/**
 * User: suresh
 * Date: 16/8/13
 * Time: 10:14 AM
 */
public class Publisher implements Closeable
{

    private static final Publisher publisher = new Publisher();
    private ZMQ.Socket pub;
    private ZMQ.Context context;
    private static Logger logger = LoggerFactory.getLogger(Publisher.class);


    private Publisher()
    {
        context = ZMQ.context(1);
        pub = context.socket(ZMQ.PUB);
        pub.bind("tcp://*:5556");
        pub.bind("ipc://cassandra");
    }

    public static Publisher getPublisher()
    {
        return publisher;
    }

    public void publish(String message)
    {
        logger.debug("publishing to ZeroMQ "+message);
        pub.send(message, 0);
    }

    public void close() throws IOException
    {
        pub.close();
        context.term ();
    }
}
