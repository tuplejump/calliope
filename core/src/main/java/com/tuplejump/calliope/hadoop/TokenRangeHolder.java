package com.tuplejump.calliope.hadoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class TokenRangeHolder implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(TokenRangeHolder.class);

    private String startToken;
    private String endToken;
    private long length;

    public TokenRangeHolder() {

    }

    public TokenRangeHolder(String startToken, String endToken, long length) {
        this.startToken = startToken;
        this.endToken = endToken;
        this.length = length;
    }

    public String getStartToken() {
        return startToken;
    }

    public void setStartToken(String startToken) {
        this.startToken = startToken;
    }

    public String getEndToken() {
        return endToken;
    }

    public void setEndToken(String endToken) {
        this.endToken = endToken;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

}
