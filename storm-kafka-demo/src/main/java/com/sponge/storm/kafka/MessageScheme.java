package com.sponge.storm.kafka;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

public class MessageScheme implements Scheme {
    private static final Logger logger = LoggerFactory.getLogger(MessageScheme.class);

    public List<Object> deserialize(ByteBuffer byteBuffer) {
        String msg = getString(byteBuffer);
        logger.info("get one message is {}", msg);
        return new Values(msg);
    }

    public Fields getOutputFields() {
        return new Fields("msg");
    }

    public static String getString(ByteBuffer buffer) {

        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;
        try {

            charset = Charset.forName("UTF-8");
            decoder = charset.newDecoder();
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            return charBuffer.toString();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return "";
        }

    }
}
