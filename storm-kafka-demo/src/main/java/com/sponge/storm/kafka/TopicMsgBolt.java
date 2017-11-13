package com.sponge.storm.kafka;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicMsgBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(TopicMsgBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = (String) input.getValue(0);
        String out = "Message got is '" + word + "'!";
        logger.info("out={}", out);
        collector.emit(new Values(out));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}
