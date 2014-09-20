package com.tuplejump.calliope.hadoop;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class MultiRangeSplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(MultiRangeSplit.class);

    private long totalLength;
    private int tokensInRange;
    private String[] tokenStarts;
    private String[] tokenEnds;
    private long[] tokenLengths;
    private String[] dataNodes;

    public MultiRangeSplit() {
    }

    public MultiRangeSplit(long totalLength, int tokensInRange, String[] tokenStarts, String[] tokenEnds, long[] tokenLengths, String[] dataNodes) {
        this.totalLength = totalLength;
        this.tokensInRange = tokensInRange;
        this.tokenStarts = tokenStarts;
        this.tokenEnds = tokenEnds;
        this.tokenLengths = tokenLengths;
        this.dataNodes = dataNodes;
    }

    public MultiRangeSplit(TokenRangeHolder[] tokenRanges, long length, String[] dataNodes) {
        tokensInRange = tokenRanges.length;

        tokenStarts = new String[tokensInRange];
        tokenEnds = new String[tokensInRange];
        tokenLengths = new long[tokensInRange];

        for (int i = 0; i < tokensInRange; i++) {
            tokenStarts[i] = tokenRanges[i].getStartToken();
            tokenEnds[i] = tokenRanges[i].getEndToken();
            tokenLengths[i] = tokenRanges[i].getLength();
        }
        totalLength += length;
        this.dataNodes = dataNodes;
    }

    public TokenRangeHolder[] getTokenRanges() {
        TokenRangeHolder[] trs = new TokenRangeHolder[tokensInRange];
        for (int i = 0; i < tokensInRange; i++) {
            trs[i] = new TokenRangeHolder(tokenStarts[i], tokenEnds[i], tokenLengths[i]);
        }
        return trs;
    }

    @Override
    public long getLength() {
        return totalLength;
    }

    @Override
    public String[] getLocations() {
        return dataNodes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        try {
            out.writeLong(totalLength);
            out.writeInt(tokensInRange);
            for (String tokenStart : tokenStarts) {
                out.writeUTF(tokenStart);
            }

            for (String tokenEnd : tokenEnds) {
                out.writeUTF(tokenEnd);
            }

            for (long tokenLen : tokenLengths) {
                out.writeLong(tokenLen);
            }

            out.writeInt(dataNodes.length);
            for (String node : dataNodes) {
                out.writeUTF(node);
            }
        } catch (IOException ex) {
            logger.error("Error in writing out split");
            throw ex;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            this.totalLength = in.readLong();
            this.tokensInRange = in.readInt();

            this.tokenStarts = new String[tokensInRange];

            for (int i = 0; i < tokensInRange; i++) {
                tokenStarts[i] = in.readUTF();
            }

            this.tokenEnds = new String[tokensInRange];
            for (int i = 0; i < tokensInRange; i++) {
                tokenEnds[i] = in.readUTF();
            }

            this.tokenLengths = new long[tokensInRange];
            for (int i = 0; i < tokensInRange; i++) {
                tokenLengths[i] = in.readLong();
            }

            int numOfEndpoints = in.readInt();
            dataNodes = new String[numOfEndpoints];
            for (int i = 0; i < numOfEndpoints; i++) {
                dataNodes[i] = in.readUTF();
            }

        } catch (IOException ex) {
            logger.error("Error in reading out split");
            throw ex;
        }

    }
}
